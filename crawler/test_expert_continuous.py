"""Continuous expert admission, backfill and recovery tests; no live traffic."""
import asyncio
import dataclasses
import datetime as dt
import json
import tempfile
import unittest
from pathlib import Path

from .expert_pool import ExpertPool, latest_finished_season
from .expert_continuous import ExpertCrawler
from .selftest import _fast_cfg, MockFetcher
from .test_season import manifest, battle_page, START, END
from .parsers import parse_battles,parse_next_battles_page
from .client import PatchrightFetcher, SessionCurlFetcher, FetcherError, BrowserSessionRecovery, FetchResult
from .config import CrawlConfig
from unittest.mock import AsyncMock, patch


class ExpertPoolTests(unittest.TestCase):
    def test_history_parser_chooses_older_page_not_previous_page(self):
        html=('<a href="/player/OWNER/battles/history?before=2000"><i class="angle left icon"></i></a>'
              '<a href="/player/OWNER/battles/history?before=1000&amp;"><i class="angle right icon"></i></a>')
        self.assertEqual(parse_next_battles_page(html,'https://royaleapi.com'),
                         'https://royaleapi.com/player/OWNER/battles/history?before=1000')

    def test_cap_evidence_reopen_and_conflict_rollback(self):
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'pool.db'
            p=ExpertPool(path,2)
            rows=[{'tag':t,'rank':r,'rank_season':'2026-08'} for t,r in [('ABC',1),('DEF',10000),('GHI',2)]]
            self.assertEqual(len(p.admit(rows,'https://example.test','f'*64,latest='2026-08')),2)
            self.assertEqual(len(ExpertPool(path,2).members()),2)
            for change in ({'rank':10001},{'rank':True},{'rank_season':'2026-09'}):
                with self.assertRaises(ValueError):
                    p.admit([{**rows[0],**change}],'url','sha',latest='2026-08')
            with self.assertRaises(ValueError):
                p.admit([{**rows[0],'rank':7}],'url','sha',latest='2026-08')
            self.assertEqual(p.members()['ABC']['rank'],1)
            p.offer(['JKL'],'source')
            self.assertEqual(p.stats()['candidates'],0)

    def test_candidates_are_not_members_and_retry_is_persisted(self):
        with tempfile.TemporaryDirectory() as tmp:
            p=ExpertPool(Path(tmp)/'pool.db')
            p.offer(['#ABC','ABC','DEF'],'source')
            self.assertEqual(p.stats()['candidates'],2)
            self.assertEqual(p.members(),{})
            p.finish('candidates','ABC','temporary timeout')
            self.assertEqual(p.due('candidates')['tag'],'DEF')

    def test_first_monday_settlement(self):
        self.assertEqual(latest_finished_season(dt.datetime(2026,9,6,tzinfo=dt.timezone.utc)),'2026-07')
        self.assertEqual(latest_finished_season(dt.datetime(2026,9,7,9,tzinfo=dt.timezone.utc)),'2026-08')


class ExpertCrawlerTests(unittest.IsolatedAsyncioTestCase):
    async def test_registered_sessions_enable_from_real_verification_without_restart(self):
        from .proxy_pool import ProxyState
        from .ratelimit import TokenBucket
        c=self.c
        c.pool.states=[ProxyState('http://local-a',TokenBucket(1,1),enabled=False),ProxyState('http://local-b',TokenBucket(1,1),enabled=False)]
        c.session_registry=self.root/'registry.json'
        c.session_registry.write_text(json.dumps({'results':[{'proxy':'http://local-a','status':'ready','http_status':200},{'proxy':'http://local-b','status':'login_required','http_status':200}]}))
        await c.reload_session_registry(force=True)
        self.assertEqual([p.enabled for p in c.pool.states],[True,False])
        c.session_registry.write_text(json.dumps({'results':[{'proxy':p.url,'status':'ready','http_status':200} for p in c.pool.states]}))
        await c.reload_session_registry(force=True)
        self.assertTrue(all(p.enabled for p in c.pool.states))

    async def asyncSetUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.root=Path(self.temp.name)
        roster=self.root/'roster.json';roster.write_text(json.dumps(manifest()),encoding='utf-8')
        cfg=_fast_cfg(str(self.root/'data'))
        cfg=dataclasses.replace(cfg,min_battle_timestamp=START,max_battle_timestamp=None,
            discover_players=False,adaptive_discovery=True,seeds_file=None,max_pages_per_player=10000,
            list_pause_free_memory_gb=0,list_resume_free_memory_gb=0.01,
            source_refresh=dataclasses.replace(cfg.source_refresh,enabled=True,urls=[]))
        self.c=ExpertCrawler(cfg,str(roster),10000,MockFetcher({}))

    async def asyncTearDown(self):
        self.c.close();self.temp.cleanup()

    def test_only_verified_members_and_august_to_current(self):
        c=self.c
        with self.assertRaises(ValueError):
            c._seed_to_tasks([('OUTSIDER','unverified')])
        self.assertEqual(len(c._seed_to_tasks([('OWNER','verified')])),1)
        metadata=parse_battles(battle_page([('BATTLE','OTHER',END+20)]),'https://royaleapi.com',
                              'https://royaleapi.com/player/OWNER/battles')[0]['metadata']
        self.assertTrue(c._eligible_metadata(metadata))
        self.assertFalse(c._eligible_metadata({**metadata,'timestamp':START-1}))
        self.assertFalse(c._eligible_metadata({**metadata,'team_tags':['STRANGER'],'opponent_tags':['OTHER']}))
        self.assertEqual(c.expert_sides(metadata)[0]['rank_season'],'2026-08')
        self.assertFalse(c._season_list_url_allowed('https://evil.test/player/OWNER/battles','OWNER'))

    async def test_duplicate_backfill_does_not_truncate_and_revisit_is_scheduled(self):
        c=self.c
        await c._seed([('OWNER','verified')])
        task=(await c.store.pending(1))[0]
        html=battle_page([('BATTLE','OTHER',END-60)],cursor=(END-60)*1000)
        battles=parse_battles(html,c.cfg.base_url,task['url'])
        # No new IDs: the historical chain must still continue.
        await c._expand_novel_frontier(task,html,battles,battles,'')
        rows=c.store.conn.execute("SELECT url FROM tasks WHERE status='pending'").fetchall()
        self.assertEqual(len(rows),1)
        self.assertIn('before=',rows[0]['url'])
        self.assertEqual(c.store.conn.execute('SELECT COUNT(*) FROM player_revisits').fetchone()[0],1)
        self.assertEqual(c.experts.stats()['candidates'],1)
        self.assertNotIn('OTHER',c.members)

    async def test_bad_cursor_keeps_task_for_recovery(self):
        c=self.c
        await c._seed([('OWNER','verified')])
        task=(await c.store.pending(1))[0]
        html=battle_page([('BATTLE','OTHER',END-60)],cursor=99999999999999)
        battles=parse_battles(html,c.cfg.base_url,task['url'])
        with self.assertRaises(ValueError):
            await c._expand_novel_frontier(task,html,battles,battles,'')
        self.assertEqual((await c.store.stats()).get('inflight'),1)

    async def test_first_history_anchor_tolerates_small_server_clock_skew(self):
        import time
        c=self.c;await c._seed([('OWNER','verified')]);task=(await c.store.pending(1))[0]
        cursor=int((time.time()+2)*1000)
        html=battle_page([('BATTLE','OTHER',END-60)],cursor=cursor)
        battles=parse_battles(html,c.cfg.base_url,task['url'])
        await c._expand_novel_frontier(task,html,battles,battles,'')
        self.assertTrue(c.store.conn.execute("SELECT 1 FROM tasks WHERE status='pending' AND url LIKE '%before=%'").fetchone())

    async def test_history_is_scheduled_even_with_many_pending_roots(self):
        c=self.c;await c._seed([('OWNER','verified')])
        for i in range(12):
            await c.store.add(f'https://royaleapi.com/player/OWNER/battles?root={i}','source','list',{'tag':'OWNER','page':1},f'root-{i}')
        await c.store.add(f'https://royaleapi.com/player/OWNER/battles/history?before={END*1000}','source','list',{'tag':'OWNER','page':2},'history')
        rows=await c.store.pending_authoritative_fair(limit=5,ready_limit=1,list_limit=5)
        self.assertTrue(any('/history?' in row['url'] for row in rows))

    async def test_explicit_missing_player_is_deferred_without_losing_membership(self):
        import time
        c=self.c;await c._seed([('OWNER','verified')]);task=(await c.store.pending(1))[0]
        html='<div class="ui negative message"><div class="header">Player tag #OWNER not found (404)</div></div>'
        await c._expand_novel_frontier(task,html,[],[],'')
        row=c.store.conn.execute('SELECT status,next_retry_at FROM tasks WHERE id=?',(task['id'],)).fetchone()
        self.assertEqual(row['status'],'pending')
        self.assertGreater(row['next_retry_at'],time.time()+5*3600)
        self.assertIn('OWNER',c.members)
        self.assertEqual(c._stats['player_unavailable'],1)

    async def test_missing_player_marker_must_match_requested_player(self):
        c=self.c;await c._seed([('OWNER','verified')]);task=(await c.store.pending(1))[0]
        html='<div class="ui negative message"><div class="header">Player tag #OTHER not found (404)</div></div>'
        with self.assertRaises(ValueError):await c._expand_novel_frontier(task,html,[],[],'')

    async def test_frozen_pool_keeps_backfill_and_revisit_without_candidates(self):
        c=self.c;c.dynamic_settings={'expand_player_pool':False}
        await c._seed([('OWNER','verified')])
        task=(await c.store.pending(1))[0]
        html=battle_page([('BATTLE','OTHER',END-60)],cursor=(END-60)*1000)
        battles=parse_battles(html,c.cfg.base_url,task['url'])
        with patch.object(c.experts,'offer',side_effect=AssertionError('frozen pool must not write candidates')):
            await c._expand_novel_frontier(task,html,battles,battles,'')
        self.assertEqual(c.experts.stats()['candidates'],0)
        self.assertTrue(c.store.conn.execute("SELECT 1 FROM tasks WHERE status='pending' AND url LIKE '%before=%'").fetchone())
        self.assertEqual(c.store.conn.execute('SELECT COUNT(*) FROM player_revisits').fetchone()[0],1)

    async def test_frozen_sources_do_not_fetch_or_register_discovery(self):
        c=self.c;c.dynamic_settings={'expand_player_pool':False}
        with patch.object(c.experts,'register_sources',side_effect=AssertionError('frozen')),patch.object(c,'_fetch_raw',new=AsyncMock()) as fetch:
            await c._live_sources.initialize()
            await c._live_sources.tick()
            fetch.assert_not_awaited()
        self.assertTrue(c._live_sources.state['player_pool_frozen'])

    async def test_empty_run_contract_can_resume_without_old_queue(self):
        # Keep workers idle by completing the single root; discovery disabled
        # only for this bounded lifecycle test.
        c=self.c
        c._live_sources=None
        result=await c.run([])
        self.assertEqual(result['stats']['ok'],0)
        self.assertEqual(c._phase,'completed')


class ClientRecoveryTests(unittest.IsolatedAsyncioTestCase):
    async def test_list_browser_parks_on_lightweight_same_origin_page(self):
        f=PatchrightFetcher(dataclasses.replace(CrawlConfig(),backend='session_curl'))
        page=AsyncMock();page.content.return_value='<html><title>RoyaleAPI</title></html>'
        await f._warm_up(page)
        self.assertEqual([call.args[0] for call in page.goto.await_args_list],
                         [f.cfg.base_url,f.cfg.base_url.rstrip('/')+'/robots.txt'])

    async def test_list_warmups_do_not_load_all_homepages_at_once(self):
        f=PatchrightFetcher(CrawlConfig());gate=asyncio.Event();entered=asyncio.Event()
        active=0;maximum=0
        async def warm(page):
            nonlocal active,maximum
            active+=1;maximum=max(maximum,active)
            if active==2:entered.set()
            await gate.wait();active-=1
        f._warm_up_page=warm
        tasks=[asyncio.create_task(f._warm_up(object())) for _ in range(8)]
        await asyncio.wait_for(entered.wait(),1)
        self.assertEqual(active,2)
        gate.set();await asyncio.gather(*tasks)
        self.assertEqual(maximum,2)

    async def test_session_transport_override_is_used_and_preserved(self):
        from types import SimpleNamespace
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);cookie=root/'lane.json';mapping=root/'map.json'
            cookie.write_text(json.dumps({'cookies':[{'name':'session','value':'old','domain':'royaleapi.com','path':'/'}],
                'http_impersonate':'firefox147','http_user_agent':'Recorded Firefox UA'}))
            mapping.write_text(json.dumps({'local':str(root/'lane-profile')}))
            h=SessionCurlFetcher(dataclasses.replace(CrawlConfig(),ruyi_auth_map_file=str(mapping)))
            fake=SimpleNamespace(close=AsyncMock())
            with patch.object(h._cr,'AsyncSession',return_value=fake) as constructor:
                h._session_for('local')
                self.assertEqual(constructor.call_args.kwargs['impersonate'],'firefox147')
                self.assertEqual(constructor.call_args.kwargs['headers']['User-Agent'],'Recorded Firefox UA')
                fake.cookies=constructor.call_args.kwargs['cookies']
                fake.cookies.set('session','renewed',domain='royaleapi.com',path='/')
                await h._persist_authenticated_session('local',fake)
            stored=json.loads(cookie.read_text())
            self.assertEqual(stored['http_impersonate'],'firefox147')
            self.assertEqual(stored['http_user_agent'],'Recorded Firefox UA')
            await h.aclose()

    async def test_cookie_domains_and_server_refresh_survive_restart(self):
        from curl_cffi import requests
        from types import SimpleNamespace
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'session.json'
            rows=[{'name':'session','value':'old','domain':'royaleapi.com','path':'/'}]
            path.write_text(json.dumps({'cookies':rows}))
            h=object.__new__(SessionCurlFetcher)
            h._cr=requests;h.cfg=CrawlConfig();h._persist_locks={}
            h._cookie_files={'local':path};h._cookie_rows={'local':rows};h._cookie_stamps={};h._cookies={}
            jar=h._cookie_jar(rows)
            jar.set('session','renewed',domain='royaleapi.com',path='/')
            self.assertEqual(len(list(jar.jar)),1)
            await h._persist_authenticated_session('local',SimpleNamespace(cookies=jar))
            saved=json.loads(path.read_text())['cookies']
            self.assertEqual(saved[0]['domain'],'royaleapi.com')
            self.assertEqual(saved[0]['value'],'renewed')
            self.assertEqual(h._cookie_jar(saved).get('session'),'renewed')

    async def test_browser_restart_restores_exported_session_cookie(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            cookie=root/'lane.json';mapping=root/'map.json'
            cookie.write_text(json.dumps({'cookies':[{'name':'session','value':'local-test','domain':'.royaleapi.com','path':'/','expiry':-1}]}))
            mapping.write_text(json.dumps({'local':str(root/'lane-profile')}))
            cfg=dataclasses.replace(CrawlConfig(),backend='session_curl',ruyi_auth_map_file=str(mapping))
            browser=PatchrightFetcher(cfg);context=AsyncMock()
            await browser._restore_session_cookies(context,'local')
            context.add_cookies.assert_awaited_once()
            self.assertEqual(context.add_cookies.call_args.args[0][0]['name'],'session')
            self.assertNotIn('expires',context.add_cookies.call_args.args[0][0])

    async def test_login_uses_browser_but_429_does_not(self):
        http=AsyncMock();browser=AsyncMock();browser._states={}
        recovery=BrowserSessionRecovery(http,browser)
        http.fetch.return_value=FetchResult('url',200,json.dumps({'success':False,'html':'requires login'}),{})
        browser.fetch.return_value=FetchResult('url',200,json.dumps({'success':True,'html':'replay'}),{})
        r=await recovery.fetch('local','https://example.test/data/replay?tag=ABC')
        self.assertTrue(r.json()['success'])
        browser.fetch.assert_awaited_once()
        http.fetch.return_value=FetchResult('url',429,'slow down',{})
        r=await recovery.fetch('local','https://example.test/data/replay?tag=ABC')
        self.assertEqual(r.status_code,429)
        browser.fetch.assert_awaited_once()

    async def test_browser_timeout_discards_context(self):
        f=PatchrightFetcher(CrawlConfig())
        page=AsyncMock();page.evaluate.side_effect=asyncio.TimeoutError
        context=AsyncMock()
        state={'page':page,'context':context,'request_lock':asyncio.Lock(),'last_request_at':0}
        f._ensure=AsyncMock(return_value=state)
        with self.assertRaises(FetcherError):
            await f.fetch(None,'https://example.test')
        self.assertIsNone(state['page'])
        context.close.assert_awaited_once()

    async def test_renewed_cookie_is_reloaded_without_logging_values(self):
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'session.json'
            path.write_text(json.dumps({'cookies':[{'name':'session','value':'new-value'}]}))
            f=object.__new__(SessionCurlFetcher)
            f._cookie_poll=0
            f._cookie_files={'local':path}
            f._cookie_stamps={'local':(0,0)}
            f._cookies={'local':{'session':'old'}}
            f._sessions={}
            f._reload_cookies()
            self.assertEqual(f._cookies['local']['session'],'new-value')


if __name__=='__main__':unittest.main()
