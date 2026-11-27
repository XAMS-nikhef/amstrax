"""Corrections workflow of the online loop (2026-11-27, amstrax_files docs/corrections_workflow.md): the corrections
version from the run database (--corrections_version db) and the provisional-gains flag. No database or network."""

import sys
import types
from pathlib import Path

AP_DIR = Path(__file__).resolve().parents[1] / "amstrax" / "auto_processing_new"
sys.path.insert(0, str(AP_DIR))
import auto_processing as ap  # noqa: E402


def settings(doc=None, fail=False):
    def find_one(q):
        if fail:
            raise RuntimeError("no database")
        return doc if doc and q.get("name") == doc.get("name") else None
    return types.SimpleNamespace(find_one=find_one)


def args(v):
    return types.SimpleNamespace(corrections_version=v)


def test_explicit_version_is_used_as_given():
    assert ap.resolve_corrections_version(args("PROD6"), settings(fail=True)) == "PROD6"
    assert ap.resolve_corrections_version(args(None), settings(fail=True)) is None


def test_db_version_from_settings():
    doc = {"name": "online_corrections_version", "value": "ONLINE2"}
    assert ap.resolve_corrections_version(args("db"), settings(doc)) == "ONLINE2"


def test_db_version_default_without_document():
    assert ap.resolve_corrections_version(args("db"), settings()) == "ONLINE"


def test_db_unreadable_keeps_previous():
    assert ap.resolve_corrections_version(args("db"), settings(fail=True), previous="PROD6") == "PROD6"
    assert ap.resolve_corrections_version(args("db"), settings(fail=True)) == "ONLINE"


FILES = {
    "_global_ONLINE.json": {"gain_to_pe_array": "gain_to_pe_array_online.json"},
    "_global_PROD6.json": {"gain_to_pe_array": "gain_to_pe_array_v8.json"},
    "_global_DEV.json": {"gain_to_pe_array": "gain_to_pe_array_dev.json"},
    "gain_to_pe_array_online.json": {"007790-007962": [1], "007963-999999": [2]},
    "gain_to_pe_array_dev.json": {"007790-*": [3]},
    "gain_to_pe_array_v8.json": {"007963-999999": [2]},
}


def get(name):
    if name not in FILES:
        raise FileNotFoundError(name)
    return FILES[name]


def test_provisional_only_in_the_open_range_of_online_gains():
    assert ap.provisional_gains("ONLINE", 8100, get) is True
    assert ap.provisional_gains("ONLINE", 7800, get) is False          # own (closed) range
    assert ap.provisional_gains("DEV", 7800, get) is True               # '*'
    assert ap.provisional_gains("PROD6", 8100, get) is False           # frozen version: fixed gains
    assert ap.provisional_gains(None, 8100, get) is False
    assert ap.provisional_gains("MISSING", 8100, get) is False         # files not readable: no flag


def test_submission_passes_the_iteration_version(monkeypatch):
    sent = {}
    monkeypatch.setattr(ap, "submit_job", lambda **k: sent.setdefault("job", k["jobstring"]))
    monkeypatch.setattr(ap, "update_processing_status", lambda *a, **k: None)
    monkeypatch.setattr(ap, "append_processing_history", lambda *a, **k: sent.setdefault("hist", k))
    monkeypatch.setattr(ap, "set_provisional_gains", lambda r, p, v, **k: sent.setdefault("flag", (p, v)))
    monkeypatch.setattr(ap, "provisional_gains", lambda v, r: v == "ONLINE")
    a = types.SimpleNamespace(max_jobs=5, production=True, target=["events"], output_folder="/x", corrections_version="db",
                              amstrax_path=None, set_config_kwargs={}, set_context_kwargs={}, logs_path="/l", queue="short",
                              mem=1)
    col = types.SimpleNamespace(find=lambda q, p: [])
    ap.submit_new_jobs(a, col, [{"number": 8100}], "/a", corrections_version="ONLINE")
    assert "--corrections_version ONLINE" in sent["job"] and "--corrections_version db" not in sent["job"]
    assert sent["hist"]["corrections_version"] == "ONLINE" and sent["hist"]["provisional_gains"] is True
    assert sent["flag"] == (True, "ONLINE")
