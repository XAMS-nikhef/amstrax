import os
import tempfile
import subprocess
import shlex

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
    log="job.log",
    jobname="somejob",
    queue="short",
    condor_file=None,
    dry_run=False,
    mem_per_cpu=1000,
    cpus_per_task=1,
    log_dir="logs",  # Log directory
    **kwargs,
):
    """
    Submit a job using HTCondor.

    :param jobstring: the command to execute
    :param log: where to store the log file of the job
    :param jobname: the name of the job
    :param queue: the queue to submit the job to
    :param condor_file: the file to write the condor submission script to
    :param dry_run: if True, do not submit the job
    :param mem_per_cpu: the memory per cpu in MB
    :param cpus_per_task: the number of cpus per task
    :param log_dir: directory where log and error files are stored
    :return:
    """
    # Ensure log directory exists
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # Define log, output, and error file paths in the same log directory
    if log.endswith(".log"):
        log_dir = os.path.dirname(log)
    log_file = os.path.join(log_dir, f"{jobname}.log")
    output_file = os.path.join(log_dir, f"{jobname}.out")
    error_file = os.path.join(log_dir, f"{jobname}.err")
    # Use a persistent directory for the job script
    job_executable = os.path.join(log_dir, f"{jobname}.sh")

    # Create the job executable script
    with open(job_executable, "w") as job_script:
        job_script.write("#!/bin/bash\n")
        job_script.write("echo 'Starting job...'\n")
        job_script.write(f"{jobstring}\n")
        job_script.write("echo 'Job complete.'\n")

    # Make the job script executable
    os.chmod(job_executable, 0o755)

    # Create the condor submission script
    condor_script = condor_template.format(
        job_executable=job_executable,
        log=log_file,
        output=output_file,
        error=error_file,
        mem_per_cpu=mem_per_cpu,
        cpus_per_task=cpus_per_task,
        queue=queue,
    )

    if dry_run:
        print("=== DRY RUN ===")
        print(condor_script)
        return

    with tempfile.NamedTemporaryFile(mode="w", suffix=".sub", delete=False) as tmp_file:
        tmp_file.write(condor_script)
        condor_file = tmp_file.name

    try:
        command = f"condor_submit {condor_file}"
        subprocess.run(shlex.split(command), check=True)
    except subprocess.CalledProcessError as e:
        print(f"An error occurred while submitting the job: {e}")
    finally:
        if os.path.exists(condor_file):
            os.remove(condor_file)
