import argparse
import os

from amstrax import amstrax_dir

script_template = """#!/bin/bash
export PATH=/project/xenon/jorana/software/miniconda3/bin:$PATH
source activate /data/xenon/joranang/anaconda/envs/amstrax_2021
cd /data/xenon/{detector}/processing_staged_run
echo "starting script!"
which python
python {amstrax_dir}/auto_processing/{script}.py {arguments} --detector {detector}
echo "Script complete, bye!"
"""

def submit_job(run_id, 
               target,
               context,
               detector,
               job_folder='jobs', 
               log_folder='logs',
               script = 'amstraxer'
              ):
    """
    This script will save data to
    /data/xenon/{detector}/processing_staged_run/amstrax_data
    """
    for folder in (job_folder, log_folder):
        if not os.path.exists(folder):
            os.makedirs(folder)

    # Build a script to submit to stoomboot cluster
    script_name = os.path.join(os.path.abspath('.'),
                               job_folder,
                               f'p_{run_id}_{target}_{context}.sh')
    log_file = os.path.join(os.path.abspath('.'),
                            log_folder,
                            f'p_{run_id}_{target}_{context}.log')

    arguments = f' {run_id} --target {target} --context {context}'
    script_file = open(script_name, 'w')
    script_file_content = script_template.format(
        arguments=arguments,
        detector=detector,
        amstrax_dir=amstrax_dir,
        script=script,
    )
    script_file.write(script_file_content)
    script_file.close()

    # Submit the job
    command = f'qsub {script_name} -j oe -o {log_file}'
    print(command)
    os.system(command)
    print(f'Submitted job for run {run_id}:{target}:{context}')

def parse_args():
    parser = argparse.ArgumentParser(
        description='Submit single run',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '--run_id',
        type=str,
        help="ID of the run to process; usually the run name.")
    parser.add_argument(
        '--target',
        default='raw_records',
        help="Target final data type to produce.")
    parser.add_argument(
        '--context',
        default='xams_little',
        help="xams_little or xams")
    parser.add_argument(
        '--detector',
        default='xamsl',
        help="xamsl or xams")
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    submit_job(run_id=f'{int(args.run_id):06}', target=args.target, context=args.context, detector=args.detector)
