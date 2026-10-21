"""get_mongo_client() reuses one MongoClient per connection instead of
creating a new one per call (which leaked threads and sockets in the
long-running dashboard, October 2026). No database or tunnel needed."""

import amstrax.rundb as rundb


class FakeClient:
    made = 0

    def __init__(self, uri):
        FakeClient.made += 1
        self.uri = uri
        self.closed = False

    def close(self):
        self.closed = True

    def __getitem__(self, name):
        return {"runs_gas": f"{self.uri}/{name}/runs_gas", "runs_new": "new"}


def patch(monkeypatch, port=41227):
    FakeClient.made = 0
    monkeypatch.setattr(rundb, "_CLIENTS", {})
    monkeypatch.setattr(rundb.pymongo, "MongoClient", FakeClient)
    monkeypatch.setattr(rundb, "get_configs", lambda: {
        "DAQ_PASSWORD": "x", "DAQ_HOST": "h", "DAQ_USER": "u",
        "MONGO_USER": "m", "MONGO_PASSWORD": "p", "MONGO_PORT": "27017"})
    monkeypatch.setattr(rundb, "establish_ssh_tunnel", lambda *a, **k: port)


def test_repeated_calls_share_one_client(monkeypatch):
    patch(monkeypatch)
    clients = {id(rundb.get_mongo_client()) for _ in range(50)}
    for _ in range(50):
        rundb.get_mongo_collection()
    assert len(clients) == 1
    assert FakeClient.made == 1


def test_a_new_tunnel_port_gets_its_own_client(monkeypatch):
    patch(monkeypatch, port=1111)
    a = rundb.get_mongo_client()
    monkeypatch.setattr(rundb, "establish_ssh_tunnel", lambda *x, **k: 2222)
    b = rundb.get_mongo_client()
    assert a is not b and FakeClient.made == 2


def test_a_forked_process_does_not_reuse_the_parent_client(monkeypatch):
    patch(monkeypatch)
    a = rundb.get_mongo_client()
    monkeypatch.setattr(rundb.os, "getpid", lambda: -1)
    b = rundb.get_mongo_client()
    assert a is not b


def test_clients_are_closed_at_exit(monkeypatch):
    patch(monkeypatch)
    c = rundb.get_mongo_client()
    monkeypatch.setattr(rundb, "_TUNNELS", {})
    rundb._close_mongo_connections()
    assert c.closed
