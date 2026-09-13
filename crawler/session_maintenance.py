"""Bounded session repair, triggered by replay authentication failures."""
from __future__ import annotations

import asyncio
import collections
import json
import logging
import subprocess
import sys
import time
from pathlib import Path

from .dynamic_lanes import newest_replay_url
from .storage import Storage

log=logging.getLogger('crawler.session_maintenance')


class SessionMaintenance:
    def __init__(self,crawler,max_parallel=2):
        self.crawler=crawler
        self._slots=asyncio.Semaphore(max_parallel)
        self._jobs={}
        self._states={}
        self._next_attempt={}
        self._registry_lock=asyncio.Lock()
        self.succeeded=0
        self.failed=0
        self.max_parallel=max_parallel

    def schedule(self,state,reason):
        # List browsers have separate ProxyState objects and their own recovery.
        if not any(state is p for p in self.crawler.pool.states):return
        if self.crawler._stop.is_set() or not state.enabled:return
        if state.url in self._jobs or time.monotonic()<self._next_attempt.get(state.url,0):return
        http=getattr(self.crawler.fetcher,'http',self.crawler.fetcher)
        path=getattr(http,'_cookie_files',{}).get(state.url)
        if path is None:return
        name=Path(path).stem
        self._states[state.url]={'session':name,'phase':'queued','reason':reason}
        task=asyncio.create_task(self._recover(state,name,http,state.rate_limited),name='recover-'+name)
        self._jobs[state.url]=task
        task.add_done_callback(lambda _task,proxy=state.url:self._jobs.pop(proxy,None))

    async def _publish_ready(self,state,name,http,rate_limited_before):
        if self.crawler._stop.is_set():return
        old=getattr(http,'_sessions',{}).pop(state.url,None)
        if old is not None:
            try:await asyncio.wait_for(old.close(),10)
            except Exception:log.warning('Old HTTP connection cleanup timed out for %s',name)
        http._cookie_poll=0
        http._reload_cookies()
        state.healthy=True
        state.consecutive_failures=0
        # A successful login must never erase a newer server 429 backoff.
        if state.rate_limited==rate_limited_before:
            state.cooldown_until=max(0,state.rate_limit_until)
        path=getattr(self.crawler,'session_registry',None)
        if path:
            async with self._registry_lock:
                data=json.loads(path.read_text(encoding='utf-8-sig'))
                for row in data.get('results',[]):
                    if row['proxy']==state.url and row.get('status')=='ready':
                        row.update(http_status=200,checked_at=time.time())
                data['counts']=dict(collections.Counter(r['status'] for r in data.get('results',[])))
                await asyncio.to_thread(Storage.atomic_text,path,json.dumps(data,ensure_ascii=False,indent=2))
        self.succeeded+=1
        self._states[state.url]={'session':name,'phase':'ready','updated_at':time.time()}
        self._next_attempt[state.url]=time.monotonic()+30
        log.info('Session %s automatically recovered; HTTP replay verified',name)

    async def _recover(self,state,name,http,rate_limited_before):
        process=None
        try:
            async with self._slots:
                if self.crawler._stop.is_set():return
                if time.monotonic()<state.rate_limit_until or (state.rate_limited!=rate_limited_before and time.monotonic()<state.cooldown_until):
                    self._states[state.url]['phase']='deferred_rate_limit'
                    self._next_attempt[state.url]=state.cooldown_until
                    return
                self.crawler._refresh_list_memory_guard()
                memory=self.crawler._list_memory_guard.snapshot().get('available_gib')
                if memory is not None and memory<6:
                    self._states[state.url]['phase']='deferred_memory'
                    self._next_attempt[state.url]=time.monotonic()+120
                    return
                root=Path(self.crawler.cfg.output_dir)
                url=await asyncio.to_thread(newest_replay_url,root/'index.jsonl',self.crawler.cfg.base_url)
                if not url:return
                directory=root/'session-maintenance';directory.mkdir(exist_ok=True)
                log_path=directory/(name+'.log')
                workspace=Path(__file__).resolve().parent.parent
                self._states[state.url].update(phase='browser_running',updated_at=time.time())
                with log_path.open('ab') as output:
                    process=await asyncio.create_subprocess_exec(
                        sys.executable,'-m','crawler.session_login','--name',name,
                        '--proxy',state.url,'--replay-url',url,'--reuse-only','--auto-cf',
                        '--timeout-minutes','2',cwd=str(workspace),stdout=output,stderr=output,
                        creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
                    self._states[state.url]['pid']=process.pid
                    code=await asyncio.wait_for(process.wait(),200)
                status_path=workspace/'data/auth_sessions'/(name+'.login-status.json')
                status=json.loads(status_path.read_text(encoding='utf-8')) if status_path.exists() else {}
                if code==0 and status.get('phase')=='ready' and status.get('last_replay_status')==200:
                    await self._publish_ready(state,name,http,rate_limited_before)
                else:
                    self.failed+=1
                    self._states[state.url].update(phase='failed',last_phase=status.get('phase'),updated_at=time.time())
                    self._next_attempt[state.url]=time.monotonic()+300
                    if status.get('phase') in ('google_login_required','saved_session_expired') or status.get('cf_attempts',0)>=3:
                        self._states[state.url]['requires_user']=True
                    log.warning('Session %s automatic recovery did not pass: %s',name,status.get('phase','no_result'))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.failed+=1
            self._states[state.url].update(phase='failed',error_type=type(exc).__name__,updated_at=time.time())
            self._next_attempt[state.url]=time.monotonic()+300
            log.warning('Session %s recovery failed: %s',name,type(exc).__name__)
        finally:
            if process is not None and process.returncode is None:
                if sys.platform=='win32':
                    killer=await asyncio.create_subprocess_exec('taskkill','/PID',str(process.pid),'/T','/F',
                        stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,
                        creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
                    await killer.wait()
                else:process.terminate()
                await process.wait()

    def snapshot(self):
        return {'enabled':True,'max_parallel':self.max_parallel,'pending':len(self._jobs),
                'succeeded':self.succeeded,'failed':self.failed,'sessions':list(self._states.values())}

    async def aclose(self):
        tasks=list(self._jobs.values())
        for task in tasks:task.cancel()
        if tasks:await asyncio.gather(*tasks,return_exceptions=True)
