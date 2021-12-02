import os
import argparse


script_template = """#!/bin/bash
export PATH=/data/xenon/joranang/anaconda/bin:$PATH
source activate amstrax_2021
cd /data/xenon/xamsl/processing_stage
echo "starting script!" > {log_file} 2>&1
which python  > {log_file} 2>&1
python /data/xenon/xamsl/software/amstrax/amstrax/autoprocessing/process_run.py {arguments} > {log_file} 2>&1  # noqa
echo "Script complete, bye!" > {log_file} 2>&1
"""


def submit_job(run_id, target, job_folder='./jobs', log_folder ='./logs'):
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
        log_file=log_file,
    )
    script_file.write(script_file_content)
    script_file.close()

    # Submit the job
    os.system('qsub %s' % script_name)
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
