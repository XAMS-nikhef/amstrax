import os

script_template = """#!/bin/bash
export PATH=/data/xenon/joranang/anaconda/bin:$PATH
source activate amstrax_2021
cd /data/xenon/xamsl/processing_stage
python /data/xenon/xamsl/software/amstrax/amstrax/autoprocessing/process_run.py {arguments} > {log_file} 2>&1  # noqa
echo "Script complete, bye!"
"""


def submit_job(run_name, target, job_folder='./jobs', log_folder = './logs'):
    for folder in (job_folder, log_folder):
        if not os.path.exists(folder):
            os.makedirs(folder)

    # Build a script to submit to stoomboot cluster
    script_name = os.path.join(job_folder,
                               f'p_{run_name}_{target}.sh')
    log_file = os.path.join(log_folder, f'p_{run_name}_{target}.log')

    arguments = f'{run_name} --target {target}'

    script_file = open(script_name, 'w')
    script_file_content = script_template.format(
        arguments=arguments,
        log_file=log_file,
    )
    script_file.write(script_file_content)
    script_file.close()

    # Submit the job
    os.system('qsub %s' % script_name)
    print(f'Submitted job for run {run_name}:{target}')
