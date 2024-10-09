"""
Carlo Fuselli
cfuselli@nikhef.nl
-------------------

Module that handles job submission on stoomboot, adapted from utilix sbatchq (XENON)

For more information on the queue, see:
https://www.nikhef.nl/pdp/computing-course/batch/stoomboot.html

Queue	Default Length	Max Length
express	10m	10m
generic	24h	24h
gpu-nv	24h	96h
gpu-amd	24h	96h
long	48h	96h
multicore	96h	96h
short	4h	4h

"""

import argparse
import os
import tempfile
import subprocess
import shlex


sbatch_template = """
executable              = {executable}
arguments               = --channel 1 --run_ids 006712,006713
log                     = /data/xenon/xams_v2/users/mflierm/notebooks/logs/{job_name}.log
output                  = /data/xenon/xams_v2/users/mflierm/notebooks/logs/{job_output}.txt
error                   = /data/xenon/xams_v2/users/mflierm/notebooks/logs/{job_errors}.txt
request_memory          = {memory}
## Can use "el7", "el8", or "el9" for UseOS or you can specify your own 
## SingularityImage but an OS must be specified and in string quotations. 
+UseOS                  = "el9"
## This job can run up to 4 hours. Can choose "express", "short", "medium", or "long".
+JobCategory            = {queue}
queue

"""

TMPDIR = os.path.join(os.environ.get("user", "."), "tmp")


def submit_job(
    executable:str,
    log="job.log",
    job_name="somejob",
    job_output="someoutput",
    job_errors="someerrors",
    queue="express",
    hours=None,
    memory="500M",
    **kwargs
):
    """

    See XENONnT utilix function sbatcth for info

    :param jobstring: the command to execute
    :param log: where to store the log file of the job
    :param jobname: the name of the job
    :param queue: the queue to submit the job to
    :param sbatch_file: the file to write the sbatch script to
    :param dry_run: if True, do not submit the job
    :param mem_per_cpu: the memory per cpu in MB
    :param cpus_per_task: the number of cpus per task
    :param hours: the number of hours to run the job for
    :param kwargs: additional arguments to pass to the sbatch script
    :return:

    """

    sbatch_script = sbatch_template.format(
        executable=executable,
        job_name=job_name,
        job_output=job_output,
        job_errors=job_errors,
        log=log,
        queue=queue,
        memory=memory,
    )

    if dry_run:
        print("=== DRY RUN ===")
        print(sbatch_script)
        return

    with tempfile.NamedTemporaryFile(mode='w', suffix=".sh", delete=False) as tmp_file:
        tmp_file.write(sbatch_script)
        sbatch_file = tmp_file.name

    try:
        command = "qsub %s" % sbatch_file
        subprocess.run(shlex.split(command), check=True)
    except subprocess.CalledProcessError as e:
        print(f"An error occurred while submitting the job: {e}")
    finally:
        if os.path.exists(sbatch_file):
            os.remove(sbatch_file)
