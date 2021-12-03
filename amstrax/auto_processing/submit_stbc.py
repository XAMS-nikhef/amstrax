import os
import argparse
from amstrax import amstrax_dir

script_template = """#!/bin/bash
export PATH=/project/xenon/jorana/software/miniconda3/bin:$PATH
source activate /data/xenon/joranang/anaconda/envs/amstrax_2021
cd /data/xenon/xamsl/processing_stage
echo "starting script!"
which python
python {amstrax_dir}/auto_processing/process_run.py {arguments}
echo "Script complete, bye!"
"""


def submit_job(run_id, target, job_folder='./jobs', log_folder='./logs'):
    for folder in (job_folder, log_folder):
        if not os.path.exists(folder):
            os.makedirs(folder)

    # Build a script to submit to stoomboot cluster
    script_name = os.path.join(os.path.abspath('.'),
                               job_folder,
                               f'p_{run_id}_{target}.sh')
    log_file = os.path.join(os.path.abspath('.'),
                            log_folder,
                            f'p_{run_id}_{target}.log')

    arguments = f'{run_id} --target {target}'

    script_file = open(script_name, 'w')
    script_file_content = script_template.format(
        arguments=arguments,
        amstrax_dir=amstrax_dir,
    )
    script_file.write(script_file_content)
    script_file.close()

    # Submit the job
    os.system(f'qsub {script_name} -e e{log_file} -o o{log_file}')
    print(f'Submitted job for run {run_id}:{target}')


def parse_args():
    parser = argparse.ArgumentParser(
        description='Submit single run',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '--run_id',
        type=str,
        help="Name of context to use")
    parser.add_argument(
        '--target',
        default='raw_records',
        help="Name of context to use")
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    submit_job(run_id=f'{int(args.run_id):06}', target=args.target)
