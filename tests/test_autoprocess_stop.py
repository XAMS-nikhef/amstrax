"""Clean stop of the online auto-processing loop (October 2026).

Ctrl-C / SIGTERM set a stop flag: the loop finishes the step it is in, never
stops between a condor submission and the run-document update, and leaves its
sleep at once. No database, condor or tunnel needed.
"""

import os
import signal
import subprocess
import sys
import textwrap
import time
import types
from pathlib import Path

import pytest

AP_DIR = Path(__file__).resolve().parents[1] / "amstrax" / "auto_processing_new"
sys.path.insert(0, str(AP_DIR))
import auto_processing as ap  # noqa: E402


@pytest.fixture(autouse=True)
def fresh(monkeypatch):
    ap.STOP.clear()
    fake = types.SimpleNamespace(
        amstrax_dir="/x", get_mongo_collection=lambda: object(),
        get_mongo_client=lambda: {"daq": {"processing": types.SimpleNamespace(
            find_one=lambda q: {"status": "on"})}})
    monkeypatch.setattr(ap, "amstrax", fake, raising=False)
    monkeypatch.setattr(ap, "handle_running_jobs", lambda *a, **k: None)
    yield
    ap.STOP.clear()


def args(**kw):
    a = dict(timeout=20, output_folder="/o", target=["raw_records"], run_id=None, max_jobs=5,
             production=False, corrections_version=None, amstrax_path=None, set_config_kwargs={},
             set_context_kwargs={}, queue="short", mem=8000, logs_path="/l")
    a.update(kw)
    return types.SimpleNamespace(**a)


def test_stop_leaves_the_sleep_at_once(monkeypatch):
    calls = []

    def no_runs(*a):
        calls.append(1)
        ap.STOP.set()          # as if Ctrl-C arrived during this check
        return []

    monkeypatch.setattr(ap, "update_task_list", no_runs)
    t0 = time.monotonic()
    ap.main(args(timeout=20))
    assert time.monotonic() - t0 < 2        # not the 20-s nap
    assert calls == [1]


def test_stop_between_submissions_finishes_the_current_one(monkeypatch):
    submitted = []
    runs = [{"number": n} for n in (7001, 7002, 7003)]

    class Col:
        def find(self, *a, **k):
            return []

    def fake_submit(*a, **k):
        submitted.append(1)
        ap.STOP.set()          # stop arrives while the first submission runs

    monkeypatch.setattr(ap, "submit_job", fake_submit)
    monkeypatch.setattr(ap, "update_processing_status", lambda *a, **k: None, raising=False)
    monkeypatch.setattr(ap, "append_processing_history", lambda *a, **k: None, raising=False)
    ap.submit_new_jobs(args(production=True), Col(), runs, "/x")
    assert len(submitted) == 1


def test_sigint_stops_a_running_loop_cleanly(tmp_path):
    """A real SIGINT to a process running the loop: it exits by itself, quickly."""
    script = tmp_path / "run.py"
    script.write_text(textwrap.dedent(f"""
        import sys, types
        sys.path.insert(0, {str(AP_DIR)!r})
        import auto_processing as ap
        ap.amstrax = types.SimpleNamespace(amstrax_dir="/x", get_mongo_collection=lambda: object(),
            get_mongo_client=lambda: {{"daq": {{"processing": types.SimpleNamespace(find_one=lambda q: {{"status": "on"}})}}}})
        ap.handle_running_jobs = lambda *a, **k: None
        ap.update_task_list = lambda *a: []
        ap.install_stop_handlers()
        print("ready", flush=True)
        ap.main(types.SimpleNamespace(timeout=30, output_folder="/o", target=["raw_records"], run_id=None, production=False))
        print("clean exit", flush=True)
    """))
    p = subprocess.Popen([sys.executable, str(script)], stdout=subprocess.PIPE, text=True)
    assert p.stdout.readline().strip() == "ready"
    time.sleep(0.3)
    t0 = time.monotonic()
    p.send_signal(signal.SIGINT)
    out, _ = p.communicate(timeout=10)
    assert p.returncode == 0
    assert "clean exit" in out
    assert time.monotonic() - t0 < 5
