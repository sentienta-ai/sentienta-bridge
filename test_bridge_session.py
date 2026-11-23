import base64
import json
import threading
import unittest

import bridge_session as bs


def token(exp):
    body = base64.urlsafe_b64encode(json.dumps({"exp": exp}).encode()).decode().rstrip("=")
    return "h." + body + ".s"


class Reply:
    def __init__(self, status, body):
        self.status_code, self._body = status, body

    def json(self):
        return self._body


class Query:
    def __init__(self, team, query, user, headers):
        self.team_name, self.query_id, self.user_id, self.auth_headers = team, query, user, headers


class OwnerSessionTests(unittest.TestCase):
    def setUp(self):
        self.calls = []

    def post(self, url, headers=None, data=None, timeout=None):
        self.calls.append((url, headers, json.loads(data)))
        if "cognito-idp" in url:
            return Reply(200, {"AuthenticationResult": {"IdToken": token(10_000)}})
        return Reply(200, {"message": {"queues": [{"teamName": "agent-task:wr_v3_read_1", "queryID": "brq_1",
                                                   "bridgeID": "desktop_browser"}]}})

    def session(self, id_token=""):
        return bs.OwnerSession(user_id="owner", refresh_token="R", client_id="C", region="us-west-2",
                               v3_endpoint="https://api.example.com/v3/v3-query", id_token=id_token, post=self.post)

    def test_valid_token_is_reused_and_expiring_one_is_renewed(self):
        s = self.session(token(5_000))
        self.assertEqual(s.current_id_token(now=1_000), token(5_000))
        self.assertEqual(self.calls, [])
        self.assertEqual(s.current_id_token(now=4_900), token(10_000))
        url, headers, body = self.calls[0]
        self.assertEqual(url, "https://cognito-idp.us-west-2.amazonaws.com/")
        self.assertEqual(body["AuthFlow"], "REFRESH_TOKEN_AUTH")
        self.assertEqual(body["AuthParameters"]["REFRESH_TOKEN"], "R")

    def test_maintain_registers_pending_work_and_refreshes_queue_credentials(self):
        bs.install(self.session(token(5_000)))
        existing = Query("v3-account-services", "v3-account:owner", "owner", {"Authorization": "Bearer old"})
        queues = {("v3-account-services", "v3-account:owner"): existing}
        added = bs.maintain(queues, threading.Lock(), lambda t, q, u, h: Query(t, q, u, h),
                            lambda tok: {"Authorization": "Bearer " + tok}, log=lambda m: None, now=1_000)
        self.assertEqual(added, 1)
        self.assertIn(("agent-task:wr_v3_read_1", "brq_1"), queues)
        self.assertEqual(existing.auth_headers, {"Authorization": "Bearer " + token(5_000)})
        self.assertEqual(self.calls[-1][1]["Authorization"], "Bearer " + token(5_000))
        # Discovery is paced: an immediate second call does nothing.
        self.assertEqual(bs.maintain(queues, threading.Lock(), None, None, log=lambda m: None, now=1_001), 0)
        # Once v3 stops listing the queue, it is retired after the grace period.
        self.post = lambda url, headers=None, data=None, timeout=None: Reply(200, {"message": {"queues": []}})
        bs.current()._post = self.post
        bs.maintain(queues, threading.Lock(), None, lambda tok: {}, log=lambda m: None, now=1_100)
        self.assertIn(("agent-task:wr_v3_read_1", "brq_1"), queues)
        bs.maintain(queues, threading.Lock(), None, lambda tok: {}, log=lambda m: None, now=1_300)
        self.assertNotIn(("agent-task:wr_v3_read_1", "brq_1"), queues)
        self.assertIn(("v3-account-services", "v3-account:owner"), queues)
        bs.install(None)

    def test_no_session_does_nothing(self):
        bs.install(None)
        self.assertEqual(bs.maintain({}, threading.Lock(), None, None), 0)


if __name__ == "__main__":
    unittest.main()
