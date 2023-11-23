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


sbatch_template = """#!/bin/bash

#PBS -N {jobname}
#PBS -j oe
#PBS -o {log}
#PBS -l pmem={mem_per_cpu}
#PBS -q {queue}

echo "Starting script!"
echo `date`

{job}

echo "Script complete, bye!"
echo `date`

"""

TMPDIR = os.path.join(os.environ.get("user", "."), "tmp")


def submit_job(
    jobstring,
    log="job.log",
    jobname="somejob",
    queue="generic",
    sbatch_file=None,
    dry_run=False,
    mem_per_cpu=1000,
    cpus_per_task=1,
    hours=None,
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
        jobname=jobname,
        log=log,
        job=jobstring,
        queue=queue,
        mem_per_cpu=mem_per_cpu,
        cpus_per_task=cpus_per_task,
        hours=hours,
    )

    if dry_run:
        print("=== DRY RUN ===")
        print(sbatch_script)
        return

    if sbatch_file is None:
        remove_file = True
        _, sbatch_file = tempfile.mkstemp(suffix=".sh")
    else:
        remove_file = False

    with open(sbatch_file, "w") as f:
        f.write(sbatch_script)

    command = "qsub %s" % sbatch_file
    if not sbatch_file:
        print("Executing: %s" % command)
    subprocess.Popen(shlex.split(command)).communicate()

    if remove_file:
        os.remove(sbatch_file)