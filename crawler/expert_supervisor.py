"""Local supervisor for continuous expert collection; logs and stop file stay local."""
from __future__ import annotations

import argparse
import json
import logging
from logging.handlers import RotatingFileHandler
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time

from .run_lock import RunLock
from .storage import Storage


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--settings',required=True)
    args=p.parse_args()
    path=Path(args.settings).resolve()
    settings=json.loads(path.read_text(encoding='utf-8-sig'))
    root=Path(settings['output_dir']).resolve();root.mkdir(parents=True,exist_ok=True)
    logging.basicConfig(level=logging.INFO,handlers=[RotatingFileHandler(root/'supervisor.log',maxBytes=2*1024*1024,backupCount=3,encoding='utf-8')],
                        format='%(asctime)s %(levelname)s %(message)s')
    state={'supervisor_pid':os.getpid(),'phase':'starting','restarts':0,'started_at':time.time()}
    def publish(**values):
        state.update(values,updated_at=time.time())
        Storage.atomic_text(root/'supervisor.json',json.dumps(state,ensure_ascii=False,indent=2))
    with RunLock(root/'.supervisor.lock'):
        child=None
        try:
            while not (root/'STOP').exists():
                free=shutil.disk_usage(root).free
                if free<20*1024**3:
                    publish(phase='waiting_for_disk',free_bytes=free)
                    time.sleep(30);continue
                failure_count=state['restarts']
                errors_path=root/'process-errors.log'
                if errors_path.exists() and errors_path.stat().st_size>2*1024*1024:
                    errors_path.replace(root/'process-errors.previous.log')
                with errors_path.open('ab') as errors:
                    child=subprocess.Popen([sys.executable,'-m','crawler.expert_continuous','--settings',str(path)],
                        cwd=str(Path(__file__).resolve().parent.parent),stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,
                        stderr=errors,creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
                publish(phase='running',child_pid=child.pid,reason=None)
                born=time.time();last_activity=born;last_counts=None;request_stop_at=None
                while child.poll() is None:
                    now=time.time()
                    try:
                        metrics=json.loads((root/'lanes/crawler-proxies.json').read_text(encoding='utf-8'))
                    except (OSError,ValueError):metrics={}
                    if float(metrics.get('updated_at',0))<born:
                        metrics={}
                    stats=metrics.get('stats',{})
                    counts=(metrics.get('total_done',0),stats.get('list_pages',0),stats.get('detail_enqueued',0))
                    if counts!=last_counts:
                        last_activity=now;last_counts=counts
                    work=metrics.get('tasks',{})
                    stale=now-float(metrics.get('updated_at',born))>240
                    stalled=(work.get('pending',0)+work.get('inflight',0)>0 and now-last_activity>1200)
                    low=shutil.disk_usage(root).free<10*1024**3
                    external_stop=(root/'STOP').exists()
                    if external_stop or stale or stalled or low:
                        if request_stop_at is None:
                            reason='user_stop' if external_stop else ('low_disk' if low else ('stale_heartbeat' if stale else 'stalled_20_minutes'))
                            publish(phase='stopping',reason=reason)
                            (root/'STOP').touch()
                            request_stop_at=now
                            logging.warning('Stopping child: %s',reason)
                        elif now-request_stop_at>90:
                            child.terminate()
                    publish(last_metrics_at=metrics.get('updated_at'),total_done=metrics.get('total_done',0),
                            pool=metrics.get('live_sources',{}).get('verified'),free_bytes=shutil.disk_usage(root).free)
                    time.sleep(5)
                publish(child_exit=child.returncode,child_pid=None)
                if (root/'STOP').exists():
                    if state.get('reason') in ('stale_heartbeat','stalled_20_minutes','low_disk'):
                        (root/'STOP').unlink()
                    else:
                        break
                state['restarts']=failure_count+1
                publish(phase='restart_backoff')
                delay=min(600,30*2**min(state['restarts']-1,4))
                for _ in range(delay//5):
                    if (root/'STOP').exists():break
                    time.sleep(5)
        finally:
            if child is not None and child.poll() is None:
                (root/'STOP').touch()
                try:child.wait(timeout=30)
                except subprocess.TimeoutExpired:child.terminate();child.wait(timeout=10)
            publish(phase='stopped',child_pid=None)


if __name__=='__main__':main()
