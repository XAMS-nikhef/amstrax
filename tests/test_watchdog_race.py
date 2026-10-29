"""Watchdog of the online auto-processing (October 2026): a run whose job finishes between the watchdog's query and
its condor check (status 'done', condor job gone) must not be marked failed. No database or condor needed."""

import sys
import types
from datetime import datetime
from pathlib import Path

AP_DIR = Path(__file__).resolve().parents[1] / "amstrax" / "auto_processing_new"
sys.path.insert(0, str(AP_DIR))
import auto_processing as ap  # noqa: E402


class FakeRuns:
    """One run document; `finish_after_find` sets it to 'done' right after the watchdog has read it."""

    def __init__(self, status, finish_after_find=False):
        self.doc = {"number": 7824, "processing_status": {"status": status, "time": datetime.now()}}
        self.finish_after_find = finish_after_find

    def find(self, query, projection):
        found = [dict(self.doc, processing_status=dict(self.doc["processing_status"]))]
        if self.finish_after_find:
            self.doc["processing_status"] = {"status": "done", "time": datetime.now()}
        return found

    def update_one(self, flt, update):
        ps = self.doc["processing_status"]
        match = (flt["processing_status.status"] == ps["status"] and flt["processing_status.time"] == ps["time"])
        if match:
            self.doc["processing_status"] = update["$set"]["processing_status"]
        return types.SimpleNamespace(modified_count=int(match))


def run_watchdog(monkeypatch, runs):
    history = []
    monkeypatch.setattr(ap, "is_job_running_in_condor", lambda run: False)
    monkeypatch.setattr(ap, "append_processing_history", lambda *a, **k: history.append((a, k)))
    ap.handle_running_jobs(runs, production=True)
    return history


def test_finished_job_is_not_marked_failed(monkeypatch):
    runs = FakeRuns("running", finish_after_find=True)
    history = run_watchdog(monkeypatch, runs)
    assert runs.doc["processing_status"]["status"] == "done"
    assert history == []


def test_missing_job_is_marked_failed(monkeypatch):
    runs = FakeRuns("running")
    history = run_watchdog(monkeypatch, runs)
    assert runs.doc["processing_status"]["status"] == "failed"
    assert len(history) == 1
