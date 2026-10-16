"""job_submission: the job script activates the environment through the setup file, not a hard-coded conda env."""
import os
import subprocess

from amstrax.auto_processing_new import job_submission as js


def _script(tmp_path, **kw):
    js.submit_job("echo payload", jobname="t", log_dir=str(tmp_path), dry_run=True, **kw)
    return (tmp_path / "t.sh").read_text()


def test_default_setup_file(tmp_path):
    text = _script(tmp_path)
    assert f"source {js.DEFAULT_SETUP_FILE}" in text
    assert "anaconda/xams" not in text and "source activate" not in text
    assert "echo payload" in text


def test_custom_setup_file_is_sourced_before_the_payload(tmp_path):
    setup = tmp_path / "setup_test.sh"
    setup.write_text("export XAMS_JOB_TEST=from_setup\n")
    js.submit_job('echo "env=$XAMS_JOB_TEST"', jobname="t2", log_dir=str(tmp_path), dry_run=True, setup_file=str(setup))
    out = subprocess.run(["bash", str(tmp_path / "t2.sh")], capture_output=True, text=True, check=True).stdout
    assert "env=from_setup" in out
    assert os.access(tmp_path / "t2.sh", os.X_OK)
