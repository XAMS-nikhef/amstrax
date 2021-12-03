#!/bin/bash
conda deactivate
export PATH=/data/xenon/joranang/anaconda/bin:$PATH
source activate amstrax_2021
cd /data/xenon/xamsl/processing_stage
echo "starting script!"
which python
python /data/xenon/xamsl/software/amstrax/amstrax/autoprocessing/process_run.py 002240 --target raw_records
echo "Script complete, bye!"
