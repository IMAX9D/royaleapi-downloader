"""Continuous August-to-present collection from an expanding verified expert pool."""
from __future__ import annotations

import argparse
import asyncio
import dataclasses
import datetime as dt
import hashlib
import json
import logging
from logging.handlers import RotatingFileHandler
import shutil
import time
from pathlib import Path
from urllib.parse import urlparse, parse_qs

from .config import load_config, validate_config
from .crawler import Crawler
from .discovery import page_fingerprint, canonical_list_url
from .expert_pool import ExpertPool, latest_finished_season
from .parsers import parse_player_links, parse_next_battles_page
from .season import load_roster, parse_roster_html, parse_profile_history_html, battle_timestamp, normalize_player
from .storage import Storage

log = logging.getLogger('crawler.experts')


class ExpertSources:
    def __init__(self, crawler):
        self.crawler = crawler
        self.state = {'enabled': True, 'qualified_only': True}
        self.next_source = 0
        self.next_discovery = 0
        self.next_stats = 0

    async def initialize(self):
        c = self.crawler
        month = dt.date.fromisoformat(latest_finished_season() + '-01')
        sources = [(c.cfg.base_url + '/players/leaderboard', None), (c.cfg.base_url + '/players/pro', None)]
        for _ in range(48):
            season = month.strftime('%Y-%m')
            sources.append((c.cfg.base_url + '/players/leaderboard/season/' + season, season))
            month = (month - dt.timedelta(days=1)).replace(day=1)
        if c.pool_expansion_active:
            await c._file_io.call(c.experts.register_sources, sources)
        await c._seed([(tag, 'verified-historical-pool') for tag in c.members])
        await self.tick(network=False)

    async def tick(self, network=True):
        c = self.crawler
        if (Path(c.cfg.output_dir) / 'STOP').exists():
            c._request_stop()
            return
        if shutil.disk_usage(c.cfg.output_dir).free < 10 * 1024 ** 3:
            self.state['paused_reason'] = 'disk_below_10GiB'
            c._request_stop()
            return
        if time.monotonic() >= self.next_stats:
            self.state.update(await c._file_io.call(c.experts.stats))
            self.next_stats=time.monotonic()+(30 if c.pool_expansion_active else 300)
        self.state['player_pool_frozen']=not c.pool_expansion_active
        # Live revisits retain an independent scheduling budget even while
        # there are thousands of initial backfill roots in the queue.
        if not c._refresh_list_memory_guard() and not c._backlog_paused:
            await c.store.schedule_player_revisits(8, 16, 100000, 300)
        if not network or not c.pool_expansion_active or time.monotonic() < self.next_discovery:
            return
        self.next_discovery = time.monotonic() + 10
        if c._refresh_list_memory_guard() or c._backlog_paused:
            return
        table, row = 'candidates', None
        if time.monotonic() >= self.next_source:
            row = await c._file_io.call(c.experts.due, 'sources')
            if row:
                table = 'sources'
                self.next_source = time.monotonic() + 60
        if row is None:
            row = await c._file_io.call(c.experts.due, 'candidates')
        if row is None:
            return
        key = row['url'] if table == 'sources' else row['tag']
        url = row['url'] if table == 'sources' else c.cfg.base_url + '/player/' + row['tag']
        try:
            result = await c._fetch_raw(url, list_request=True)
            if result is None:
                raise ValueError('qualification page not found')
            digest = hashlib.sha256(result.text.encode()).hexdigest()
            proofs = []
            if table == 'sources' and row['season']:
                proofs = [{**p, 'rank_season': row['season']} for p in parse_roster_html(result.text, row['season'])
                          if type(p.get('rank')) is int and 1 <= p['rank'] <= 10000]
                if not proofs:
                    raise ValueError('historical leaderboard has no ranked evidence')
            elif table == 'candidates':
                proof = parse_profile_history_html(result.text, latest_finished_season())
                if proof and proof['tag'] != row['tag']:
                    raise ValueError('profile identity differs from requested player')
                if proof:
                    proofs = [proof]
            else:
                tags = parse_player_links(result.text)
                if not tags:
                    raise ValueError('candidate source not recognizable')
                await c._file_io.call(c.experts.offer, tags, url)
            if proofs:
                proof_path = Path(c.cfg.output_dir) / 'evidence' / (digest + '.html')
                proof_path.parent.mkdir(exist_ok=True)
                await c._file_io.call(Storage.atomic_text, proof_path, result.text)
                before = set(c.members)
                c.members = await c._file_io.call(c.experts.admit, proofs, url, digest)
                new = set(c.members) - before
                await c._seed([(tag, url) for tag in sorted(new)])
                if new:
                    log.info('Verified expert pool: %d/%d (+%d)', len(c.members), c.experts.target, len(new))
            await c._file_io.call(c.experts.finish, table, key)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await c._file_io.call(c.experts.finish, table, key, f'{type(exc).__name__}: {str(exc)[:200]}')
            log.warning('Expert qualification deferred: %s', type(exc).__name__)

    async def loop(self):
        task=None
        settings=getattr(self.crawler,'dynamic_settings',{})
        if settings.get('dynamic_lanes'):
            from .dynamic_lanes import DynamicLanes
            task=asyncio.create_task(DynamicLanes(self.crawler,settings).loop(),name='dynamic-lanes')
            task.add_done_callback(self.crawler._background_finished)
        try:
            while not self.crawler._stop.is_set():
                await self.tick()
                await asyncio.sleep(5)
        finally:
            if task is not None:
                task.cancel()
                await asyncio.gather(task,return_exceptions=True)


class ExpertCrawler(Crawler):
    @property
    def pool_expansion_active(self):
        return bool(getattr(self,'dynamic_settings',{}).get('expand_player_pool',True)
                    and len(self.members)<self.experts.target)

    def __init__(self, cfg, roster_path, target=10000, fetcher=None, browser_recovery=False):
        self.members = {}
        super().__init__(cfg, fetcher)
        try:
            if browser_recovery and fetcher is None and cfg.backend == 'session_curl' and self.list_fetcher is not None:
                from .client import BrowserSessionRecovery
                self.fetcher = BrowserSessionRecovery(self.fetcher,self.list_fetcher)
            if self.pool.size == 1:
                self._fair_ready_limit = 1
            else:
                self._fair_ready_limit = max(1,min(cfg.global_concurrency-2,self.pool.size*2))
            if cfg.list_proxy_urls and self._list_proxies:
                self._list_proxies = [p for p in self._list_proxies if p.url in cfg.list_proxy_urls]
                self._list_proxy = self._list_proxies[0]
            self.experts = ExpertPool(Path(cfg.output_dir) / 'expert-pool.sqlite3', target)
            roster = load_roster(roster_path)
            proofs = [{'tag': tag, 'rank': rank, 'rank_season': roster.rank_season(tag)} for rank, tag in roster.players]
            self.members = self.experts.admit(proofs, roster.source_urls[0], roster.sha256)
            self._live_sources = ExpertSources(self)
        except BaseException:
            self.close()
            raise

    async def run(self, seeds=None, limit=None):
        if seeds:
            raise ValueError('continuous expert mode rejects unverified external seeds')
        await self.store.assert_campaign_contract({'kind': 'continuous-historical-top10000-v1',
            'min_timestamp': self.cfg.min_battle_timestamp, 'base_url': self.cfg.base_url,
            'expert_target': self.experts.target})
        try:
            return await super().run([], limit)
        finally:
            maintenance=getattr(self,'session_maintenance',None)
            if maintenance is not None:await maintenance.aclose()

    async def reload_session_registry(self, force=False):
        path=getattr(self,'session_registry',None)
        if path is None:return
        stat=path.stat();stamp=(stat.st_mtime_ns,stat.st_size)
        if not force and stamp==getattr(self,'_session_registry_stamp',None):return
        def read():return json.loads(path.read_text(encoding='utf-8'))
        data=await self._file_io.call(read)
        rows={r['proxy']:r for r in data.get('results',[])}
        changed=False
        async with self.pool._lock:
            for state in self.pool.states:
                row=rows.get(state.url,{})
                enabled=row.get('status')=='ready' and row.get('http_status')==200
                if enabled and not state.enabled:
                    state.healthy=True;state.consecutive_failures=0;changed=True
                state.enabled=enabled
                state.disabled_reason=None if enabled else row.get('status','unverified')
        http=getattr(self.fetcher,'http',self.fetcher)
        if changed and hasattr(http,'_reload_cookies'):
            http._cookie_poll=0;http._reload_cookies()
        enabled=[p for p in self.pool.states if p.enabled]
        groups={getattr(self,'session_rate_groups',{}).get(p.url,p.url) for p in enabled}
        if self._replay_global_bucket:
            self._replay_global_bucket.rate=max(0.15,min(getattr(self,'session_rate_ceiling',3.8),len(groups)*self.cfg.rate_limit.requests_per_second))
        self._fair_ready_limit=max(1,min(self.cfg.global_concurrency-self._fair_list_limit,len(enabled)*2))
        self._session_registry_stamp=stamp
        log.info('Session registry: %d registered, %d verified enabled, %d awaiting recovery',len(self.pool.states),len(enabled),len(self.pool.states)-len(enabled))

    async def _snapshot_progress(self,rate):
        await self.reload_session_registry()
        # While the replay backlog is sufficient, use all workers for replays.
        self._fair_ready_limit=(self.cfg.global_concurrency if self._backlog_paused else
            max(1,min(self.cfg.global_concurrency-self._fair_list_limit,sum(p.enabled for p in self.pool.states)*2)))
        await super()._snapshot_progress(rate)

    def _runtime_metrics_payload(self,rate):
        result=super()._runtime_metrics_payload(rate)
        result['registered_sessions']=len(self.pool.states)
        result['verified_sessions']=sum(p.enabled for p in self.pool.states)
        buckets={id(p.bucket):p.bucket for p in self.pool.states if p.available_now}
        result['available_replay_exits']=len(buckets)
        result['available_replay_capacity_rps']=round(sum(b.rate for b in buckets.values()),3)
        result['player_pool_frozen']=not self.pool_expansion_active
        result['replay_worker_limit']=self._fair_ready_limit
        result['list_worker_limit']=self._fair_list_limit
        maintenance=getattr(self,'session_maintenance',None)
        result['session_maintenance']=maintenance.snapshot() if maintenance is not None else {'enabled':False}
        return result

    def _seed_to_tasks(self, seeds):
        for tag, _ in seeds:
            if tag not in self.members:
                raise ValueError('unverified player cannot enter battle queue')
        return super()._seed_to_tasks(seeds)

    def _season_list_url_allowed(self, url, tag):
        p, b = urlparse(url), urlparse(self.cfg.base_url)
        return bool(tag in self.members and p.scheme == b.scheme and p.netloc == b.netloc and not p.username and not p.password
                    and p.path in (f'/player/{tag}/battles', f'/player/{tag}/battles/history'))

    def expert_sides(self, metadata):
        result = []
        for side in ('team', 'opponent'):
            for tag in (metadata or {}).get(side + '_tags') or []:
                tag = normalize_player(tag)
                if tag in self.members:
                    p = self.members[tag]
                    result.append({'side': side, 'tag': tag, 'rank': p['rank'], 'rank_season': p['season'],
                                   'evidence_sha256': p['sha256'], 'source_url': p['source']})
        return result

    def _eligible_metadata(self, metadata):
        stamp = battle_timestamp(metadata)
        return bool(super()._eligible_metadata(metadata) and stamp is not None and stamp <= time.time() + 300
                    and self.expert_sides(metadata))

    async def _process_list(self, task):
        # Parent validates URL/membership through the overridden hook.
        await super()._process_list(task)

    async def _expand_novel_frontier(self, task, html, battles, eligible, saved_path):
        meta = task.get('meta') or {}
        tag, url, page = meta['tag'], task['url'], int(meta.get('page', 1))
        if not battles and 'battle_list_container' not in html:
            from selectolax.parser import HTMLParser
            headers=HTMLParser(html).css('.ui.negative.message .header')
            if any(' '.join(h.text().split())==f'Player tag #{tag} not found (404)' for h in headers):
                status=await self.store.mark_retry(url,'player unavailable: site returned explicit player 404',
                                                  6*3600,task_id=task.get('id'))
                self._stats['player_unavailable']=self._stats.get('player_unavailable',0)+1
                self._stats['dead' if status=='dead' else 'retried']+=1
                return
            raise ValueError('unrecognized player page; do not mark history complete')
        candidates = {b['tag'] for b in eligible}
        new = (await self.store.list_new_battle_tags(task['id'])) & candidates
        if self.pool_expansion_active:
            discovered = {normalize_player(t) for b in battles for side in ('team_tags','opponent_tags')
                          for t in (b.get('metadata') or {}).get(side, [])}
            await self._file_io.call(self.experts.offer, sorted(discovered), url)
        next_url = parse_next_battles_page(html, self.cfg.base_url)
        cutoff, reason = False, None
        # Full first traversal reaches the August boundary even through old
        # duplicates. Revisits stop after overlap only after that first walk.
        revisit = meta.get('expert_revisit', False) or bool(meta.get('player_refresh_key'))
        zeros = int(meta.get('zero_new_chain', 0)) + 1 if not new else 0
        if next_url and self._list_url_in_version_window(next_url):
            if page >= self.cfg.max_pages_per_player:
                raise ValueError('history safety ceiling reached; manual review required')
            if not self._season_list_url_allowed(next_url, tag):
                raise ValueError('history cursor left the verified player')
            def cursor_ms(value):
                if not isinstance(value,(int,str)) or isinstance(value,bool) or not str(value).isdigit():return None
                stamp=int(value)
                return stamp*1000 if 0<stamp<10_000_000_000 else stamp if stamp>0 else None
            before = cursor_ms((parse_qs(urlparse(url).query).get('before') or [None])[0])
            after = cursor_ms((parse_qs(urlparse(next_url).query).get('before') or [None])[0])
            # The first history link is a server-time anchor, not an older
            # cursor. Subsequent history pages must strictly decrease in ms.
            if after is None or after>(time.time()+300)*1000 or (before is not None and after>=before):
                raise ValueError('history cursor is not strictly decreasing')
            cutoff = revisit and zeros >= 2
            reason = 'revisit_caught_up' if cutoff else None
            if not cutoff:
                # History is immutable at a fixed cursor. Original backfill
                # tasks remain durable while newest pages are revisited.
                await self.store.add(next_url, url, 'list', {'tag':tag,'page':page+1,
                    'expert_revisit':revisit,'zero_new_chain':zeros,'discovery_band':'fresh'}, canonical_list_url(next_url))
        await self.store.commit_list_navigation(task_id=task['id'],url=url,saved_path=saved_path,
            fingerprint=page_fingerprint(b['tag'] for b in battles),candidates=len(candidates),new_candidates=len(new),
            history_cutoff=cutoff,reason=reason,player_refresh={
                'player_key':self._player_list_dedup_key(tag),'player_tag':tag,
                'url':self.cfg.base_url + '/player/' + tag + '/battles',
                'interval':1800,'max_interval':7200} if page == 1 else None)

    async def _finalize_replay(self, task, data, metadata, *, source_kind, premerged=False):
        if not self._eligible_metadata(metadata):
            raise ValueError('expert/date admission failed before persistence')
        data['collection_cohort'] = {'kind':'continuous-historical-top10000-v1',
            'start_inclusive':self.cfg.min_battle_timestamp,'expert_pool_target':self.experts.target,
            'expert_sides':self.expert_sides(metadata)}
        await super()._finalize_replay(task,data,metadata,source_kind=source_kind,premerged=premerged)


def build_config(settings):
    cfg = load_config(settings['base_config'])
    root = Path(settings['output_dir']).resolve()
    cfg = dataclasses.replace(cfg, output_dir=str(root), db_path=str(root/'progress.sqlite3'),
        campaign_id=None, min_battle_timestamp=settings['start_timestamp'], max_battle_timestamp=None,
        max_battles=None, discover_players=False, adaptive_discovery=True, require_complete_decks=True,
        max_pages_per_player=10000, seeds_file=None, season_roster_file=None, season_id=None,
        excluded_battles_manifest=settings.get('exclude_manifest'), excluded_battles_databases=settings.get('exclude_databases',[]),
        authoritative_target=None, authoritative_upgrade_manifest=None,
        global_concurrency=settings.get('workers',20), claim_batch_size=100, queue_capacity=500,
        detail_backlog_high=settings.get('detail_backlog_high',10000),
        detail_backlog_low=settings.get('detail_backlog_low',5000),persistent_retry_delay=60,
        list_proxy_urls=settings.get('list_proxy_urls',cfg.list_proxy_urls),
        save_lists=settings.get('save_lists',cfg.save_lists),
        retry=dataclasses.replace(cfg.retry,max_retries=1000000,network_attempts=2),
        source_refresh=dataclasses.replace(cfg.source_refresh,enabled=True,urls=[],player_revisit_batch_size=8),
        proxy=dataclasses.replace(cfg.proxy,proxies=settings.get('replay_proxy_urls') or [settings.get('seed_proxy','http://127.0.0.1:18096')],proxies_file='',health_check_interval=60),
        rate_limit=dataclasses.replace(cfg.rate_limit,requests_per_second=0.15,burst=1),
        replay_global_requests_per_second=settings.get('global_replay_rate',0.15),
        impersonate='chrome124',list_pause_free_memory_gb=4,list_resume_free_memory_gb=6)
    validate_config(cfg)
    return cfg


def restore_proxy_progress(pool, path):
    """Retain recorded counters and unexpired shared cooldowns after restart."""
    try:
        previous=json.loads(Path(path).read_text(encoding='utf-8-sig'))
        elapsed=max(0,time.time()-float(previous['updated_at']))
        rows={r['proxy']:r for r in previous.get('proxies',[])}
        for state in pool.states:
            row=rows.get(state.label,{})
            for field in ('success','fail','rate_limited','forbidden','errors','auth_failures'):
                value=row.get(field)
                if type(value) is int and value>=0:setattr(state,field,value)
            remaining=max(0,float(row.get('cooldown_left',0))-elapsed)
            if remaining:pool._cooldown(state,remaining)
            limited=max(0,float(row.get('rate_limit_left',0))-elapsed)
            if limited:state.rate_limit_until=time.monotonic()+limited
    except (OSError,ValueError,KeyError,TypeError):
        log.warning('No valid previous proxy snapshot available for restart')


async def run(settings):
    cfg = build_config(settings)
    c = ExpertCrawler(cfg, settings['initial_roster'], settings.get('expert_target',10000),browser_recovery=settings.get('browser_recovery_enabled',False))
    c.dynamic_settings=settings
    c._fair_list_limit=max(1,min(cfg.global_concurrency-1,settings.get('list_workers',c._fair_list_limit)))
    if c._replay_global_bucket is not None:
        # Per-exit pacing already applies jitter. Adding it again to the global
        # gate leaves capacity idle even when replay work is queued.
        c._replay_global_bucket.jitter=0
    # Manual node/session recovery still needs one rate budget per actual IP.
    if settings.get('rate_groups'):
        from .ratelimit import TokenBucket
        buckets={}
        for state in c.pool.states:
            group=settings['rate_groups'].get(state.url,state.url)
            state.bucket=buckets.setdefault(group,TokenBucket(cfg.rate_limit.requests_per_second,1,cfg.rate_limit.jitter))
    restore_proxy_progress(c.pool,Path(cfg.output_dir)/'lanes/crawler-proxies.json')
    c.session_rate_groups=settings.get('rate_groups',{})
    c.session_rate_ceiling=settings.get('maximum_replay_rate',3.8)
    if settings.get('session_registry'):
        c.session_registry=Path(settings['session_registry'])
        for state in c.pool.states:state.enabled=False;state.disabled_reason='unverified'
        await c.reload_session_registry(force=True)
    if settings.get('auto_session_recovery',False):
        from .session_maintenance import SessionMaintenance
        c.session_maintenance=SessionMaintenance(c,max_parallel=2)
        c.pool.on_session_failure=c.session_maintenance.schedule
        for state in c.pool.states:
            if state.enabled and state.healthy and not state.available_now and state.forbidden and not state.rate_limited and not state.errors:
                c.session_maintenance.schedule(state,'challenge')
    try:
        await c.run()
    finally:
        c.close()


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--settings',required=True)
    p.add_argument('--status',action='store_true')
    p.add_argument('--dry-run',action='store_true')
    args=p.parse_args()
    settings=json.loads(Path(args.settings).read_text(encoding='utf-8-sig'))
    cfg=build_config(settings)
    if args.status:
        from .campaign import readonly_status
        print(json.dumps(readonly_status(cfg),ensure_ascii=False,indent=2));return
    if args.dry_run:
        roster=load_roster(settings['initial_roster'])
        print(json.dumps({'output':cfg.output_dir,'initial_verified':len(roster.players),
            'target':settings.get('expert_target',10000),'start':cfg.min_battle_timestamp,'end':'continuous',
            'replay_lanes':len(cfg.proxy.proxies)},ensure_ascii=False));return
    root=Path(cfg.output_dir);root.mkdir(parents=True,exist_ok=True)
    handler=RotatingFileHandler(root/'collector.log',maxBytes=10*1024*1024,backupCount=5,encoding='utf-8')
    logging.basicConfig(level=logging.INFO,handlers=[handler],format='%(asctime)s %(levelname)s %(name)s %(message)s')
    asyncio.run(run(settings))


if __name__=='__main__':
    main()
