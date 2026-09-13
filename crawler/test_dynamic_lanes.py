import json,tempfile,unittest
from pathlib import Path
from .dynamic_lanes import all_configured_nodes,newest_replay_url


class DynamicLaneTests(unittest.TestCase):
    def test_disabled_session_cannot_be_selected_despite_healthy_transport(self):
        from .proxy_pool import ProxyState
        from .ratelimit import TokenBucket
        state=ProxyState('local',TokenBucket(1,1),healthy=True,enabled=False)
        self.assertFalse(state.available_now)
    def test_all_nodes_include_previous_health_failures(self):
        providers={'subscription-1':{'proxies':[{'name':'first','alive':False},{'name':'second','alive':True}]},
                   'subscription-2':{'proxies':[{'name':'first','alive':True},{'name':'third','alive':False}]},
                   'default':{'proxies':[{'name':'not-a-subscription'}]}}
        self.assertEqual(all_configured_nodes(providers),['first','second','third'])

    def test_replay_probe_url_comes_only_from_own_verified_index(self):
        with tempfile.TemporaryDirectory() as tmp:
            p=Path(tmp)/'index.jsonl'
            good='https://royaleapi.com/data/replay?tag=TEST'
            rows=[{'kind':'battle','url':good},{'kind':'battle','url':'https://other.test/data/replay'},
                  {'kind':'list','url':'https://royaleapi.com/player/TEST/battles'}]
            p.write_text('\n'.join(json.dumps(r) for r in rows)+'\n{truncated',encoding='utf-8')
            self.assertEqual(newest_replay_url(p,'https://royaleapi.com'),good)


if __name__=='__main__':unittest.main()
