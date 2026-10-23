"""Corrections of simulated runs from their own run document (MC global versions): truth or calibrated, with the
calibration version in the address (amstrax.contexts.for_mc_run)."""
import json
from types import SimpleNamespace

import pytest

import amstrax
from amstrax import xams_config

URL = ("rundoc://?path=xamssim.corrections.calibrated.elife,xamssim.corrections.truth.elife")


def write_doc(folder, corrections):
    doc = {"number": 990001, "mode": "simulation (sept)", "xamssim": {"corrections": corrections}}
    (folder / "990001-metadata.json").write_text(json.dumps(doc))


@pytest.fixture
def rundocs(tmp_path, monkeypatch):
    monkeypatch.setattr(xams_config, "RUNDOC_DIRS", [str(tmp_path)])
    return tmp_path


def fetch(url, run="990001"):
    cfg = xams_config.XAMSConfig(default=None)
    cfg.name = "elife"
    plugin = SimpleNamespace(config={"elife": url}, run_id=run)
    return cfg.fetch(plugin)


def test_truth_then_calibrated(rundocs):
    write_doc(rundocs, {"truth": {"elife": 159000.0}})
    assert fetch(URL + "&calibration=0") == 159000.0
    write_doc(rundocs, {"truth": {"elife": 159000.0}, "calibrated": {"elife": 151200.0, "version": 1}})
    assert fetch(URL + "&calibration=1") == 151200.0


def test_wrong_calibration_version(rundocs):
    write_doc(rundocs, {"truth": {"elife": 159000.0}, "calibrated": {"elife": 151200.0, "version": 2}})
    with pytest.raises(ValueError, match="calibration version 2"):
        fetch(URL + "&calibration=1")


TRUTH = "rundoc://?path=xamssim.corrections.truth.elife"


def test_for_mc_run(rundocs):
    write_doc(rundocs, {"truth": {"elife": 159000.0}, "calibrated": {"elife": 151200.0, "version": 3}})
    st = amstrax.contexts.xams(output_folder=str(rundocs), init_rundb=False)
    st.set_config(dict(elife=URL + "&calibration=1"))
    st2 = amstrax.contexts.for_mc_run(st, "990001")
    assert st2.config["elife"] == URL + "&calibration=3"
    assert st.config["elife"] == URL + "&calibration=1"          # the original context is unchanged
    assert amstrax.contexts.for_mc_run(st, "990001", use="truth").config["elife"] == TRUTH
    assert fetch(TRUTH) == 159000.0                              # truth: no version check, the true value
    with pytest.raises(ValueError, match="use must be"):
        amstrax.contexts.for_mc_run(st, "990001", use="calibrated")


def test_for_mc_run_before_calibration(rundocs):
    write_doc(rundocs, {"truth": {"elife": 159000.0}})          # version 0: latest = truth, one lineage
    st = amstrax.contexts.xams(output_folder=str(rundocs), init_rundb=False)
    st.set_config(dict(elife=URL))
    assert amstrax.contexts.for_mc_run(st, "990001").config["elife"] == TRUTH
    assert amstrax.contexts.for_mc_run(st, "990001", use="truth").config["elife"] == TRUTH
    with pytest.raises(ValueError, match="no MC corrections"):
        amstrax.contexts.for_mc_run(amstrax.contexts.xams(output_folder=str(rundocs), init_rundb=False), "990001")
