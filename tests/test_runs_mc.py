"""Run documents of simulated runs (xams-sim, numbers 900000-999999) are looked up in run.runs_mc, data runs in
runs_gas; a stored run document file still comes first."""
from amstrax import contexts, rundb, xams_config


class FakeCollection:
    def __init__(self, name, docs):
        self.name, self.docs = name, docs

    def find_one(self, q, projection=None):
        return self.docs.get(q["number"])


def _fake_db(monkeypatch, asked):
    dbs = {"xams": FakeCollection("runs_gas", {7719: {"number": 7719, "mode": "data"}}),
           "xams_mc": FakeCollection("runs_mc", {900201: {"number": 900201, "mode": "simulation (sept)"}})}

    def get_mongo_collection(detector="xams", **kw):
        asked.append(detector)
        return dbs[detector]
    monkeypatch.setattr(rundb, "get_mongo_collection", get_mongo_collection)
    monkeypatch.setattr(xams_config, "RUNDOC_DIRS", [])


def test_is_mc_run():
    assert rundb.is_mc_run("900201") and rundb.is_mc_run(999999) and not rundb.is_mc_run("007719")
    assert not rundb.is_mc_run("not-a-number")


def test_rundoc_value_from_the_right_collection(monkeypatch):
    asked = []
    _fake_db(monkeypatch, asked)
    assert xams_config.get_rundoc_value("900201", "mode") == "simulation (sept)"
    assert xams_config.get_rundoc_value("007719", "mode") == "data"
    assert asked == ["xams_mc", "xams"]


def test_daq_reader_rundoc_from_runs_mc(monkeypatch):
    asked = []
    _fake_db(monkeypatch, asked)
    assert contexts._get_run_doc_for_daq_reader(None, "900201", "xams")["number"] == 900201
    assert asked == ["xams_mc"]
