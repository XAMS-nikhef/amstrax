# job_submission.py
import os
import subprocess
import shlex
import tempfile
import logging

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

condor_template = """
executable            = {job_executable}
log                   = {log}
output                = {output}
error                 = {error}
request_memory        = {mem_per_cpu}MB
request_cpus          = {cpus_per_task}
+UseOS                = "el9"
+JobCategory          = "{queue}"
queue
"""


def submit_job(
    jobstring,
    jobname="somejob",
    queue="short",
    mem_per_cpu=1000,
    cpus_per_task=1,
    log_dir="logs",
    dry_run=False,
):
    """
    Submit a job to HTCondor with the given job string and parameters.

    :param jobstring: Command to execute.
    :param jobname: Name of the job.
    :param queue: Queue to submit to.
    :param mem_per_cpu: Memory per CPU in MB.
    :param cpus_per_task: Number of CPUs per task.
    :param log_dir: Directory to store log and error files.
    :param dry_run: If True, print the job instead of submitting it.
    :return: None
    """
    # Ensure log directory exists
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # Define log, output, and error file paths
    log_file = os.path.join(log_dir, f"{jobname}.log")
    output_file = os.path.join(log_dir, f"{jobname}.out")
    job_executable = os.path.join(log_dir, f"{jobname}.sh")

    full_jobstring = f"""
    #!/bin/bash
    echo 'Starting job {jobname}'
    echo `date`
    export PATH=/data/xenon/cfuselli/miniconda-install/bin:$PATH
    source activate /data/xenon/xams_v2/anaconda/xams
    {jobstring}
    echo 'Job {jobname} complete.'
    """

    # Create the job executable script
    with open(job_executable, "w") as job_script:
        job_script.write(full_jobstring)

    os.chmod(job_executable, 0o755)  # Make executable

    # Create the condor submission script
    condor_script = condor_template.format(
        job_executable=job_executable,
        log=log_file,
        output=output_file,
        error=output_file,
        mem_per_cpu=mem_per_cpu,
        cpus_per_task=cpus_per_task,
        queue=queue,
    )

    if dry_run:
        log.info("=== DRY RUN ===")
        log.info(condor_script)
        return

    # Write the condor file and submit the job
    with tempfile.NamedTemporaryFile(mode="w", suffix=".sub", delete=False) as tmp_file:
        tmp_file.write(condor_script)
        condor_file = tmp_file.name

    try:
        command = f"condor_submit {condor_file}"
        subprocess.run(shlex.split(command), check=True)
        log.info(f"Job {jobname} submitted successfully.")
        log.info(f"Monitor the state of the job with `condor_q` or `condor_watch_q`.")
    except subprocess.CalledProcessError as e:
        log.error(f"Error submitting job: {e}")
    finally:
        if os.path.exists(condor_file):
            os.remove(condor_file)


def monitor_jobs():
    """
    Placeholder for monitoring jobs. Could be used to check job statuses and handle retries/failures.
    """

    # Placeholder for now
    raise NotImplementedError("Monitoring jobs not yet implemented.")
