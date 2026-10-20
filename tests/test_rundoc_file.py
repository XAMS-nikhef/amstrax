"""Run documents stored as files next to the data (simulated runs): found by get_rundoc_value without the database."""
import json

from amstrax import xams_config


def test_stored_rundoc(tmp_path, monkeypatch):
    doc = {"number": 900101, "mode": "simulation (sept)",
           "daq_config": {"channel_map": {"bottom": [0, 0], "top": [1, 4]}}}
    (tmp_path / "900101-metadata.json").write_text(json.dumps(doc))
    monkeypatch.setattr(xams_config, "RUNDOC_DIRS", [str(tmp_path)])

    def no_database(*args, **kwargs):
        raise AssertionError("the run database must not be asked for a stored run document")
    monkeypatch.setattr(xams_config.amstrax, "get_mongo_collection", no_database)

    assert xams_config._stored_rundoc("900101")["mode"] == "simulation (sept)"
    assert xams_config._stored_rundoc(900101)["number"] == 900101
    assert xams_config._stored_rundoc("007719") is None
    assert xams_config.get_rundoc_value("900101", "daq_config.channel_map")["top"] == [1, 4]
