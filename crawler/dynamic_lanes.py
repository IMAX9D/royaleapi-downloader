# 用途：原动态线路编排及共享辅助函数。
# 分类：仍被调用的线路辅助模块；使用：动态管理默认关闭；辅助函数仍被其他模块使用
# 相关文件与阅读顺序：见同目录 README.md。

"""Manage all existing sessions and continuously test all user provider nodes."""
import asyncio,json,os,subprocess,sys,time
from pathlib import Path
from urllib.parse import urlparse
from .lane_manager import _controller,_select,_exit_hash,TEST_LANE,TEST_PORT
from .proxy_pool import ProxyState
from .ratelimit import TokenBucket
from .resource_guard import available_physical_memory_bytes
from .storage import Storage


def all_configured_nodes(providers):
    return list(dict.fromkeys(r['name'] for k,v in providers.items() if k.startswith('subscription-')
        for r in v.get('proxies',[]) if isinstance(r.get('name'),str) and r['name']))


def newest_replay_url(index,base):
    try:
        with Path(index).open('rb') as f:
            f.seek(0,2);size=f.tell();f.seek(max(0,size-32000));lines=f.read().decode('utf-8','replace').splitlines()
        b=urlparse(base)
        for line in reversed(lines):
            try:r=json.loads(line)
            except ValueError:continue
            url=r.get('url','');p=urlparse(url)
            if r.get('kind')=='battle' and p.scheme==b.scheme and p.netloc==b.netloc and p.path=='/data/replay' and not p.username and not p.password:return url
    except OSError:pass
    return None


def pid_alive(pid):
    if not pid:return False
    if os.name=='nt':
        import ctypes
        h=ctypes.windll.kernel32.OpenProcess(0x1000,False,int(pid))
        if not h:return False
        code=ctypes.c_ulong();ok=ctypes.windll.kernel32.GetExitCodeProcess(h,ctypes.byref(code));ctypes.windll.kernel32.CloseHandle(h)
        return bool(ok and code.value==259)
    try:os.kill(int(pid),0);return True
    except OSError:return False


class DynamicLanes:
    def __init__(self,c,settings):
        self.c=c;self.settings=settings;self.root=Path(c.cfg.output_dir)
        self.http=getattr(c.fetcher,'http',c.fetcher)
        self.mapping=json.loads(Path(c.cfg.ruyi_auth_map_file).read_text(encoding='utf-8'))
        self.nodes={};self.slots={};self.buckets={};self.last_refresh=0;self.last_rate=0;self.limited=0;self.last_probe=0
        try:
            cached=json.loads((self.root/'exit-check.local.json').read_text(encoding='utf-8'))
            self.previously_connected={r['node'] for r in cached.get('results',[])}
        except (OSError,ValueError):self.previously_connected=set()
        self.state={'phase':'initializing','total_nodes':0,'tested':0,'connected':0,'active_lanes':c.pool.size,'managed_sessions':len(self.mapping)}

    async def initialize(self):
        try:
            old=json.loads((self.root/'dynamic-nodes.local.json').read_text(encoding='utf-8'))
            self.nodes=old.get('nodes',{});self.slots=old.get('slots',{})
        except (OSError,ValueError):pass
        proxies=(await asyncio.to_thread(_controller,'/proxies')).get('proxies',{})
        for proxy,profile in self.mapping.items():
            port=urlparse(proxy).port
            if not port or not 18080<=port<=18105:continue
            s=self.slots.setdefault(proxy,{})
            s.update(port=port,session=Path(profile).name.removesuffix('-profile'),node=proxies.get(f'lane-{port-18079:02d}',{}).get('now'))
            s.setdefault('status','pending_check');s.setdefault('due',0)
            if any(p.url==proxy for p in self.c.pool.states):s['status']='ready'
        # Reconfirm the current working exit first, then prioritize previously
        # reachable candidates. Every other configured node is still tested.
        for p in self.c.pool.states:
            slot=self.slots.get(p.url,{})
            if slot.get('node'):
                h=await asyncio.to_thread(_exit_hash,slot['port'],4)
                if h:self.nodes[slot['node']]={'status':'connected','exit_hash':h,'checked_at':time.time(),'due':time.time()+600}
        await self.publish()
        url=await self.c._file_io.call(newest_replay_url,self.c.storage.index_path,self.c.cfg.base_url)
        if url:await self.open_logins(url)

    async def publish(self):
        rows=list(self.nodes.values());sessions=list(self.slots.values());good=[r for r in rows if r.get('status')=='connected']
        self.state.update(updated_at=time.time(),tested=sum(bool(r.get('checked_at')) for r in rows),connected=len(good),
            unique_exits=len({r.get('exit_hash') for r in good}),unavailable=sum(r.get('status')=='unavailable' for r in rows),
            active_lanes=len(self.c.pool.states),managed_sessions=len(sessions),
            login_waiting=sum(s.get('status') in ('login_required','browser_required','login_open') for s in sessions),
            login_windows=sum(pid_alive(s.get('login_pid')) for s in sessions),global_replay_rate=self.c._replay_global_bucket.rate,
            sessions=[{k:s.get(k) for k in ('session','port','status','checked_at','login_pid','login_phase')} for s in sessions])
        await self.c._file_io.call(Storage.atomic_text,self.root/'dynamic-lanes.json',json.dumps(self.state,ensure_ascii=False,indent=2))
        await self.c._file_io.call(Storage.atomic_text,self.root/'dynamic-nodes.local.json',json.dumps({'nodes':self.nodes,'slots':self.slots},ensure_ascii=False))

    async def save_active(self):
        self.settings.update(replay_proxy_urls=[p.url for p in self.c.pool.states],list_proxy_urls=[p.url for p in self.c._list_proxies],global_replay_rate=self.c._replay_global_bucket.rate)
        await self.c._file_io.call(Storage.atomic_text,self.root/'settings.local.json',json.dumps(self.settings,ensure_ascii=False,indent=2))

    async def remove(self,proxy):
        async with self.c.pool._lock:
            for p in self.c.pool.states:
                if p.url==proxy:p.healthy=False
            self.c.pool.states=[p for p in self.c.pool.states if p.url!=proxy]
        self.c._list_proxies=[p for p in self.c._list_proxies if p.url!=proxy]
        self.c._list_proxy=self.c._list_proxies[0] if self.c._list_proxies else None
        await self.save_active()

    async def admit(self,proxy):
        slot=self.slots[proxy];h=self.nodes.get(slot.get('node'),{}).get('exit_hash') or proxy
        bucket=self.buckets.setdefault(h,TokenBucket(0.15,1,self.c.cfg.rate_limit.jitter))
        async with self.c.pool._lock:
            current=next((p for p in self.c.pool.states if p.url==proxy),None)
            if current is None:self.c.pool.states.append(ProxyState(proxy,bucket))
            else:current.bucket=bucket;current.healthy=True;current.consecutive_failures=0
        if not any(p.url==proxy for p in self.c._list_proxies) and len(self.c._list_proxies)<6:
            hashes={self.nodes.get(self.slots.get(p.url,{}).get('node'),{}).get('exit_hash') for p in self.c._list_proxies}
            if h not in hashes:self.c._list_proxies.append(ProxyState(proxy,TokenBucket(self.c.cfg.list_requests_per_second,1,0.2)))
        self.c._list_proxy=self.c._list_proxies[0] if self.c._list_proxies else None
        self.c._fair_ready_limit=max(1,min(self.c.cfg.global_concurrency-2,len(self.c.pool.states)*2))
        self.c._fair_list_limit=min(6,max(2,len(self.c._list_proxies)))
        slot.update(status='ready',due=time.time()+900,login_attempted=False)
        await self.save_active()

    async def verify_slot(self,proxy,url):
        slot=self.slots[proxy]
        try:
            await asyncio.wait_for(self.c._replay_global_bucket.acquire(),15)
        except asyncio.TimeoutError:
            slot['due']=time.time()+5
            return # Budget contention is not a failed node or expired login.
        self.http._cookie_poll=0;self.http._reload_cookies()
        result=await self.http.fetch(proxy,url) # original distinct session, not a copied login
        slot.update(checked_at=time.time(),due=time.time()+600)
        if result.status_code==429:
            self.c._replay_global_bucket.rate=max(0.15,self.c._replay_global_bucket.rate/2)
            self.last_rate=time.time();slot['status']='rate_limited';return
        try:payload=result.json()
        except ValueError:payload={}
        if result.status_code==200 and payload.get('success') is True:
            await self.admit(proxy);return
        slot['status']='browser_required' if result.status_code==403 else ('login_required' if 'requires login' in str(payload.get('html','')).lower() else 'replay_failed')
        await self.remove(proxy)

    async def open_logins(self,url):
        if not self.settings.get('recover_all_sessions'):return
        try:
            control=json.loads((self.root/'login-control.json').read_text(encoding='utf-8'))
        except (OSError,ValueError):control={}
        maximum=max(1,min(26,int(control.get('max_windows',1))))
        generation=control.get('generation')
        for s in self.slots.values():
            if generation and s.get('login_generation')!=generation:
                s['login_generation']=generation
                if s.get('status')!='ready' and not pid_alive(s.get('login_pid')):
                    s['login_attempted']=False
                    if s.get('status')=='login_open':s['status']='browser_required'
        opened=sum(pid_alive(s.get('login_pid')) for s in self.slots.values())
        for proxy,s in self.slots.items():
            if opened>=maximum:break
            if not control.get('open_all_requested') and available_physical_memory_bytes()<8*1024**3:break
            if s.get('status') not in ('login_required','browser_required') or s.get('login_attempted') or pid_alive(s.get('login_pid')):continue
            log_path=self.root/'session-logs';log_path.mkdir(exist_ok=True)
            command=[sys.executable,'-m','crawler.session_login','--name',s['session'],'--proxy',proxy,'--replay-url',url,'--timeout-minutes','45']
            if os.environ.get('GOOGLE_LOGIN_EMAIL') and os.environ.get('GOOGLE_LOGIN_PASSWORD'):command.append('--auto-google')
            with (log_path/(s['session']+'.log')).open('ab') as output:
                proc=subprocess.Popen(command,cwd=str(Path(__file__).resolve().parent.parent),stdout=output,stderr=output,creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
            s.update(login_pid=proc.pid,login_attempted=True,status='login_open',login_phase='opening_browser');opened+=1

    async def tick(self):
        now=time.time()
        if now-self.last_refresh>300:
            names=all_configured_nodes((await asyncio.to_thread(_controller,'/providers/proxies')).get('providers',{}))
            for name in names:self.nodes.setdefault(name,{'due':0})
            for name in self.nodes:
                if name not in names:self.nodes[name]['status']='unavailable'
            self.state['total_nodes']=len(names);self.last_refresh=now
        due=[(n,r) for n,r in self.nodes.items() if r.get('due',0)<=now]
        if due:
            node,r=min(due,key=lambda pair:(pair[0] not in self.previously_connected,pair[1].get('checked_at',0)));self.state['phase']='testing_all_nodes'
            def probe():
                _select(TEST_LANE,node);return _exit_hash(TEST_PORT,timeout=4)
            try:h=await asyncio.to_thread(probe)
            except Exception:h=None
            r.update(status='connected' if h else 'unavailable',exit_hash=h,checked_at=time.time(),due=time.time()+600)
        good=[n for n,r in self.nodes.items() if r.get('status')=='connected']
        for proxy,s in self.slots.items():
            live=next((p for p in self.c.pool.states if p.url==proxy),None);node=s.get('node')
            if good and (not node or self.nodes.get(node,{}).get('status')=='unavailable') and (live is None or not live.healthy) and not pid_alive(s.get('login_pid')):
                used=[x.get('node') for x in self.slots.values()];selected=min(good,key=lambda n:used.count(n))
                await asyncio.to_thread(_select,s['port']-18079,selected)
                old=self.http._sessions.pop(proxy,None)
                if old is not None:await old.close()
                s.update(node=selected,due=0,status='pending_check')
            if s.get('login_pid') and not pid_alive(s['login_pid']):s['login_pid']=None;s['due']=0
            profile=Path(self.mapping[proxy])
            status_file=profile.parent/(profile.name.removesuffix('-profile')+'.login-status.json')
            try:
                login_state=json.loads(status_file.read_text(encoding='utf-8'))
                if login_state.get('pid')==s.get('login_pid'):
                    s['login_phase']=login_state.get('phase')
            except (OSError,ValueError):pass
            path=self.http._cookie_files.get(proxy);stamp=path.stat().st_mtime_ns if path and path.exists() else 0
            if stamp!=s.get('cookie_stamp'):
                s['cookie_stamp']=stamp
                if s.get('status')!='ready':s['due']=0
        url=await self.c._file_io.call(newest_replay_url,self.c.storage.index_path,self.c.cfg.base_url)
        if url:await self.open_logins(url)
        if url and now-self.last_probe>7:
            candidates=[(p,s) for p,s in self.slots.items() if s.get('due',0)<=now and not pid_alive(s.get('login_pid')) and (self.nodes.get(s.get('node'),{}).get('status')=='connected' or s.get('status')=='ready')]
            if candidates:
                proxy,s=min(candidates,key=lambda pair:(pair[1].get('status')=='ready',pair[1].get('checked_at',0)))
                try:await self.verify_slot(proxy,url)
                except Exception as exc:s.update(status='network_failed',due=time.time()+120,last_error=type(exc).__name__);await self.remove(proxy)
                self.last_probe=time.time()
        if url:await self.open_logins(url)
        limited=sum(p.rate_limited for p in self.c.pool.states)
        if limited>self.limited:self.c._replay_global_bucket.rate=max(0.15,self.c._replay_global_bucket.rate/2);self.last_rate=now
        elif now-self.last_rate>180 and len(self.c.pool.states)>1:
            unique={self.nodes.get(self.slots.get(p.url,{}).get('node'),{}).get('exit_hash') or p.url for p in self.c.pool.states}
            cap=min(3.8,len(unique)*0.15)
            self.c._replay_global_bucket.rate=min(cap,max(self.c._replay_global_bucket.rate+0.15,self.c._replay_global_bucket.rate*1.25));self.last_rate=now
        self.limited=limited
        await self.publish()

    async def loop(self):
        await self.initialize()
        while not self.c._stop.is_set():
            try:await self.tick();self.state['last_error']=None
            except asyncio.CancelledError:raise
            except Exception as exc:self.state['last_error']=type(exc).__name__;await self.publish();await asyncio.sleep(10)
            await asyncio.sleep(1)
