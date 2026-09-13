"""Read-only localhost monitor for the current continuous expert dataset."""
import argparse
import json
import shutil
import sqlite3
import time
from pathlib import Path
from urllib.parse import urlparse
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer


def read_json(path):
    try:return json.loads(path.read_text(encoding='utf-8-sig'))
    except (OSError,ValueError):return {}


def snapshot(root):
    supervisor=read_json(root/'supervisor.json')
    metrics=read_json(root/'lanes/crawler-proxies.json')
    audit=read_json(root/'old-file-audit.json')
    login=read_json(root/'login-status.json')
    settings=read_json(root/'settings.local.json')
    dynamic=read_json(root/'dynamic-lanes.json')
    attention=read_json(root/'session-attention.json')
    recovery=metrics.get('session_maintenance',{})
    if any(row.get('requires_user') for row in recovery.get('sessions',[])):
        attention={'requires_user':True,'reason':'部分会话需要人工登录或验证，请查看自动恢复状态。'}
    done=metrics.get('total_done',0);pool=0;pending=0;dead=0;excluded=0;candidates=0
    db=root/'progress.sqlite3'
    if db.exists():
        c=sqlite3.connect(db.as_uri()+'?mode=ro',uri=True,timeout=1)
        try:
            rows=list(c.execute('SELECT kind,status,total FROM task_counts'))
            done=sum(n for k,s,n in rows if k=='detail' and s=='done')
            pending=sum(n for k,s,n in rows if s in ('pending','inflight'))
            dead=sum(n for k,s,n in rows if s=='dead')
            excluded=c.execute('SELECT COUNT(*) FROM excluded_battles').fetchone()[0]
        finally:c.close()
    db=root/'expert-pool.sqlite3'
    if db.exists():
        c=sqlite3.connect(db.as_uri()+'?mode=ro',uri=True,timeout=1)
        try:
            pool=c.execute('SELECT COUNT(*) FROM members').fetchone()[0]
            candidates=c.execute('SELECT COUNT(*) FROM candidates').fetchone()[0]
        finally:c.close()
    phase=supervisor.get('phase','not_started')
    age=max(0,time.time()-metrics.get('updated_at',0))
    if not settings.get('dynamic_lanes'):
        phase=metrics.get('phase','not_started') if age<30 else 'stale_unknown'
        session_audit=read_json(root/'session-audit.latest.json')
        active=set(settings.get('replay_proxy_urls',[]))
        status_map={'challenge':'browser_required','network_failed':'network_failed','login_required':'login_required'}
        sessions=[]
        for row in session_audit.get('results',[]):
            status=row.get('status')
            if status=='ready' and row.get('proxy') not in active:status='validated'
            sessions.append({'session':row.get('session'),'port':urlparse(row.get('proxy','')).port,
                             'status':status_map.get(status,status),'checked_at':row.get('checked_at')})
        ready_proxies={row.get('proxy') for row in session_audit.get('results',[])
                       if row.get('status')=='ready' and row.get('http_status')==200 and row.get('proxy') in active}
        groups={settings.get('rate_groups',{}).get(proxy,proxy) for proxy in ready_proxies}
        # Match expert_continuous.build_config's per-exit rate and registry cap.
        request_budget=min(settings.get('maximum_replay_rate',3.8),len(groups)*0.15)
        dynamic={**dynamic,'phase':'manual_review','managed_sessions':len(sessions),'active_lanes':metrics.get('verified_sessions',sum(row['status']=='ready' for row in sessions)),
                 'sessions':sessions,'unique_exits':len(groups),'global_replay_rate':request_budget,'login_windows':0,
                 'scan_summary':read_json(root/'node-scan-summary.json').get('description')}
        from .dynamic_lanes import pid_alive
        auth_root=Path(__file__).resolve().parent.parent/'data/auth_sessions'
        for status_path in auth_root.glob('session-*.login-status.json'):
            login_state=read_json(status_path)
            if login_state.get('phase') not in ('ready','timeout','cf_not_resolved') and pid_alive(login_state.get('pid')):
                for row in sessions:
                    if row['session']==login_state.get('session'):
                        row.update(status='login_open',login_phase=login_state.get('phase'))
                        dynamic['login_windows']+=1
    if settings.get('dynamic_lanes') and phase=='running' and time.time()-supervisor.get('updated_at',0)>30:phase='stale_unknown'
    logs=''
    try:
        with (root/'collector.log').open('rb') as f:
            f.seek(0,2);size=f.tell();f.seek(max(0,size-16000))
            logs='\n'.join(f.read().decode('utf-8','replace').splitlines()[-28:])
    except OSError:pass
    return {'phase':phase,'session_recovery':recovery,'done':done,'pool':pool,'target':10000,'candidates':candidates,'dynamic':dynamic,'attention':attention,
            'player_pool_frozen':metrics.get('player_pool_frozen',not settings.get('expand_player_pool',True)),
            'available_replay_exits':metrics.get('available_replay_exits'),
            'available_replay_capacity_rps':metrics.get('available_replay_capacity_rps'),
            'daily_target':settings.get('daily_target',300000),'target_rate':settings.get('daily_target',300000)/86400,
            'pending':pending,'dead':dead,'excluded':excluded,'free_gb':round(shutil.disk_usage(root).free/1e9,1),
            'heartbeat_age':round(age),'rate':metrics.get('battle_rate',0) if phase=='running' and age<15 else 0,
            'login':login.get('phase'),'restarts':supervisor.get('restarts',0),'root':str(root),
            'old_files':audit.get('unique_existing_files',0),'old_gb':round(audit.get('existing_file_bytes',0)/1e9,2),
            'updated_at':time.time(),'logs':logs,
            'lanes':[{k:p.get(k) for k in ('healthy','enabled','available','success','fail','auth_failures','rate_limited','latency_ema','cooldown_left')}
                     for p in metrics.get('proxies',[])]}


HTML='''<!doctype html><html lang="zh-CN"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>RoyaleAPI · 新数据集监测</title><style>
*{box-sizing:border-box}body{margin:0;background:#f4f6fa;color:#172338;font:15px system-ui,"Microsoft YaHei",sans-serif}main{max-width:1180px;margin:38px auto;padding:0 24px}header{display:flex;justify-content:space-between;gap:20px;align-items:center}h1{font-size:28px;margin:8px 0}p{color:#617087;line-height:1.6}.badge{border-radius:20px;padding:9px 16px;background:#dde7f5;white-space:nowrap}.cards{display:grid;grid-template-columns:repeat(4,1fr);gap:16px;margin:26px 0}.card,section{background:white;border:1px solid #e1e6ef;border-radius:14px;padding:22px}.label{color:#68768b;font-size:13px}.value{font-size:34px;font-weight:700;margin:8px 0}.note{font-size:12px;color:#778397}section{margin:16px 0}h2{font-size:17px;margin:0 0 18px}progress{width:100%;height:13px;accent-color:#2563eb}.row{display:flex;justify-content:space-between;gap:18px;margin:13px 0;flex-wrap:wrap}.warn{padding:15px 20px;background:#fff4db;border:1px solid #f4d58a;border-radius:10px;color:#825d10;margin:18px 0;display:none}table{width:100%;border-collapse:collapse;font-size:13px}td,th{text-align:left;padding:12px 8px;border-bottom:1px solid #eef1f6}th{color:#728096;font-weight:500}pre{white-space:pre-wrap;overflow-wrap:anywhere;background:#111d30;color:#cbd8ee;padding:18px;border-radius:9px;max-height:330px;overflow:auto;font:12px/1.7 Consolas,monospace}.path{font-family:Consolas,monospace;word-break:break-all}footer{font-size:12px;color:#7a8797;margin:22px 0}@media(max-width:700px){.cards{grid-template-columns:repeat(2,1fr)}header{align-items:flex-start;flex-direction:column}.value{font-size:27px}main{padding:0 14px;margin:20px auto}section{overflow:auto}}
</style><main><header><div><div class="label">ROYALEAPI / CONTINUOUS COLLECTION</div><h1>新数据集监测</h1><p>2026 年 8 月赛季起点 → 持续更新 · 历史 Top 10000 专家玩家</p></div><div id="phase" class="badge">读取中</div></header>
<div class="warn" id="warning"></div><div class="cards">
<div class="card"><div class="label">新下载回放</div><div class="value" id="done">—</div><div class="note">本批次成功落盘的唯一对局</div></div>
<div class="card"><div class="label">已核验专家</div><div class="value" id="pool">—</div><div class="note">目标 10,000 人，未核验不入池</div></div>
<div class="card"><div class="label">旧数据排除</div><div class="value" id="excluded">—</div><div class="note">对应文件已确认在本地</div></div>
<div class="card"><div class="label">剩余磁盘空间</div><div class="value"><span id="disk">—</span><small style="font-size:14px"> GB</small></div><div class="note">低于 10 GiB 停止写入</div></div></div>
<section><h2 id="poolHeading">玩家池</h2><progress id="bar" value="0" max="10000"></progress><div class="row"><span id="pooltext"></span><span id="candidates"></span></div><p>历史赛季榜单提供排名证据；候选玩家须通过主页历史结算记录核验。首次回溯不因重复对局提前截断，之后定期回访最新对局。</p></section>
<section><h2 id="sessionHeading">下载通道与会话</h2><p class="note" id="recovery"></p><div class="row"><span id="nodes">节点扫描准备中</span><span id="sessions">会话恢复准备中</span></div><div class="row"><span id="unique"></span><span id="globalrate"></span></div><table><thead><tr><th>原有 Session</th><th>本地端口</th><th>状态</th></tr></thead><tbody id="sessionRows"></tbody></table><p class="note">所有订阅节点都重新测试，旧健康标志不会跳过测试。失效线路移出下载池并定期复测；同一出口 IP 的多个 session 共享该 IP 的限速预算。账号、密码和真实节点地址不会显示在面板上。</p></section>
<section><h2>采集状态</h2><div class="row"><strong id="dailyTarget">目标 300,000 场 / 天</strong><span id="requiredRate">需要持续 3.47 场 / 秒</span></div><div class="row"><span id="pending"></span><span id="rate"></span><span id="dead"></span><span id="restarts"></span></div><table><thead><tr><th>Session 通道</th><th>可用状态</th><th>请求成功</th><th>失败</th><th>登录提示</th><th>限流</th><th>冷却</th></tr></thead><tbody id="lanes"></tbody></table><p class="note">每行是一个 Session 通道，多个通道可能共用同一出口。成功和失败统计实际下载请求，不是节点测速；两项为 0 表示尚无请求结果。同出口轮换分配任务、共享请求预算及冷却时间。连接异常表示暂时隔离、等待复测，不代表节点永久失效。目标不代表已达标，速度为短窗口实际落盘速率。</p></section>
<section><h2>保存位置</h2><div class="path" id="root"></div><p class="note">使用本地原下载器。数据目录中的 START.ps1 / STOP.ps1 / STATUS.ps1 可启动、停止或查看状态。本面板只读。</p></section>
<section><h2>最近运行记录</h2><pre id="logs">读取中…</pre></section><footer id="updated"></footer></main>
<script>const fmt=n=>Number(n||0).toLocaleString('zh-CN');const text=(id,s)=>document.getElementById(id).textContent=s;
async function refresh(){try{const r=await fetch('/api/status',{cache:'no-store'});if(!r.ok)throw Error('HTTP '+r.status);const d=await r.json();
const phases={running:'后台运行中',stopped:'采集已暂停',stopping:'正在正常停止',restart_backoff:'故障恢复等待',waiting_for_disk:'等待磁盘空间',stale_unknown:'心跳陈旧，需检查',not_started:'尚未启动'};text('phase',phases[d.phase]||d.phase);
text('done',fmt(d.done));text('pool',fmt(d.pool));text('excluded',fmt(d.excluded));text('disk',d.free_gb);document.getElementById('bar').value=d.pool;
text('dailyTarget','目标 '+fmt(d.daily_target)+' 场 / 天');text('requiredRate','需要持续 '+Number(d.target_rate).toFixed(2)+' 场 / 秒');
text('poolHeading',d.player_pool_frozen?'玩家池已固定 · 停止扩容':'玩家池扩容');const dyn=d.dynamic||{};text('sessionHeading',fmt(dyn.managed_sessions)+' 个下载通道与会话');text('nodes',dyn.scan_summary||('节点已测 '+fmt(dyn.tested)+' / '+fmt(dyn.total_nodes)+' · 连通 '+fmt(dyn.connected)));const repair=d.session_recovery||{};text('recovery',repair.enabled?('自动会话恢复：等待或进行中 '+fmt(repair.pending)+' · 本次成功 '+fmt(repair.succeeded)+' · 未通过 '+fmt(repair.failed)):'');text('sessions','会话纳管 '+fmt(dyn.managed_sessions)+' · 已接入 '+fmt(dyn.active_lanes)+' · 登录窗口 '+fmt(dyn.login_windows));text('unique','不同出口 IP：'+fmt(dyn.unique_exits)+' · 当前可用：'+fmt(d.available_replay_exits));text('globalrate','配置请求预算：'+Number(dyn.global_replay_rate||0).toFixed(2)+' / 秒 · 当前出口容量：'+Number(d.available_replay_capacity_rps||0).toFixed(2)+' / 秒');const sr=document.getElementById('sessionRows');sr.replaceChildren();const labels={validated:'HTTP已验证，待接入',recovering_cloudflare:'正在恢复CF',reading_location:'读取页面状态',checking_cloudflare:'检测CF',ready:'已通过回放验证',pending_check:'等待验证',network_failed:'网络失败，待复测',login_required:'需要登录',browser_required:'需要浏览器校验',login_open:'正在恢复登录',rate_limited:'限流冷却',replay_failed:'回放验证失败',opening_browser:'打开窗口',waiting_human_verification:'请点击人机验证',google_login:'继续 Google 登录',waiting_google_confirmation:'等待 Google 二次确认',verifying_replay:'验证真实回放'};(dyn.sessions||[]).forEach(s=>{const row=document.createElement('tr');const state=s.status==='login_open'&&s.login_phase?s.login_phase:s.status;[s.session,s.port,labels[state]||state].forEach(v=>{const td=document.createElement('td');td.textContent=v;row.appendChild(td)});sr.appendChild(row)});
text('pooltext',fmt(d.pool)+' / '+fmt(d.target)+' 名已核验');text('candidates',fmt(d.candidates)+' 名候选等待核验');text('pending','待处理 / 执行中：'+fmt(d.pending));text('rate','当前落盘：'+Number(d.rate).toFixed(2)+' 场/秒');text('dead','永久失败：'+fmt(d.dead));text('restarts','本次后台自动重启：'+fmt(d.restarts));text('root',d.root);text('logs',d.logs||'尚无日志');text('updated','每 3 秒自动刷新 · 更新于 '+new Date(d.updated_at*1000).toLocaleString('zh-CN')+' · 采集心跳距今 '+d.heartbeat_age+' 秒');
const warn=document.getElementById('warning');let message='';if(d.attention&&d.attention.requires_user&&dyn.login_windows>0)message='登录恢复遇到 Cloudflare 人机验证，尚未进入 Google 登录页。已有可用会话继续下载；未通过验证的会话不计入可用数量。';else if(d.login==='waiting_for_user_login')message='正在等待 RoyaleAPI 登录窗口完成授权。旧会话恢复仍在核查，暂停状态不代表下载完成。';else if(d.phase!=='running')message='当前采集未运行；已保存的数据和断点队列仍保留。';else if(d.heartbeat_age>30)message='采集心跳已陈旧，请检查日志和后台状态。';warn.textContent=message;warn.style.display=message?'block':'none';
const body=document.getElementById('lanes');body.replaceChildren();d.lanes.forEach((p,i)=>{let tr=document.createElement('tr');['通道 '+(i+1),p.enabled===false?'待验证':(p.available?((p.success+p.fail)===0?'尚无请求结果':'可用'):(p.healthy?'冷却中':'连接异常，待复测')),fmt(p.success),fmt(p.fail),fmt(p.auth_failures),fmt(p.rate_limited),Math.ceil(p.cooldown_left||0)+' 秒'].forEach(v=>{let td=document.createElement('td');td.textContent=v;tr.appendChild(td)});body.appendChild(tr)});
}catch(e){text('phase','面板连接异常');text('updated',String(e));}}refresh();setInterval(refresh,3000);</script></html>'''


def main():
    p=argparse.ArgumentParser();p.add_argument('--root',required=True);p.add_argument('--port',type=int,default=19741);args=p.parse_args()
    root=Path(args.root).resolve()
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path=='/':data=HTML.encode('utf-8');kind='text/html; charset=utf-8'
            elif self.path=='/api/status':
                try:data=json.dumps(snapshot(root),ensure_ascii=False).encode('utf-8')
                except Exception as exc:
                    self.send_error(503,type(exc).__name__);return
                kind='application/json; charset=utf-8'
            else:self.send_error(404);return
            self.send_response(200);self.send_header('Content-Type',kind);self.send_header('Cache-Control','no-store');self.send_header('Content-Length',str(len(data)));self.end_headers();self.wfile.write(data)
        def log_message(self,*args):pass
    ThreadingHTTPServer(('127.0.0.1',args.port),Handler).serve_forever()


if __name__=='__main__':main()
