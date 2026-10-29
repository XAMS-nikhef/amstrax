"""Watchdog of the online auto-processing loop (28-10-2026): a run is marked failed only when its job is gone from
condor, never for waiting or running long in the queue, and not within the grace period after submission.
No database or condor needed."""

import sys
import types
from datetime import datetime, timedelta
from pathlib import Path

AP_DIR = Path(__file__).resolve().parents[1] / "amstrax" / "auto_processing_new"
sys.path.insert(0, str(AP_DIR))
import auto_processing as ap  # noqa: E402


def run(monkeypatch, docs, in_condor):
    failed = []
    col = types.SimpleNamespace(find=lambda q, p: docs)
    monkeypatch.setattr(ap, "is_job_running_in_condor", lambda n: n in in_condor)
    monkeypatch.setattr(ap, "update_processing_status", lambda n, s, **k: failed.append(n))
    monkeypatch.setattr(ap, "append_processing_history", lambda *a, **k: None)
    ap.handle_running_jobs(col, production=True)
    return failed


def doc(n, minutes_ago, status="submitted"):
    return {"number": n, "processing_status": {"status": status, "time": datetime.now() - timedelta(minutes=minutes_ago)}}


def test_queued_job_is_not_failed_after_30_minutes(monkeypatch):
    assert run(monkeypatch, [doc(7843, 90), doc(7844, 45, "running")], in_condor={7843, 7844}) == []


def test_job_missing_from_condor_is_failed_after_grace(monkeypatch):
    assert run(monkeypatch, [doc(7792, 10)], in_condor=set()) == [7792]


def test_just_submitted_job_is_left_alone(monkeypatch):
    assert run(monkeypatch, [doc(7872, 0.3)], in_condor=set()) == []


def test_condor_match_uses_the_job_name(monkeypatch):
    out = "ID OWNER SUBMITTED RUN_TIME ST PRI SIZE CMD\n6017843.0 xamsdata 10/28 18:00 0+00:01:00 R 0 0.0 process_007792_online.sh\n"
    monkeypatch.setattr(ap.shutil, "which", lambda c: "/usr/bin/condor_q")
    monkeypatch.setattr(ap.subprocess, "run", lambda *a, **k: types.SimpleNamespace(returncode=0, stdout=out, stderr=""))
    assert ap.is_job_running_in_condor(7792) is True
    assert ap.is_job_running_in_condor(7843) is False          # '7843' appears in the cluster id, not in the job name
