import pytest
import argparse
from unittest.mock import patch
from amstrax.auto_processing_new.offline_processing import parse_args, main

@patch('amstrax.auto_processing_new.offline_processing.submit_job')
@patch('amstrax.auto_processing_new.offline_processing.input', return_value='y')
def test_main(mock_input, mock_submit_job):
    # Mock arguments
    args = [
        '--run_id', '123', 
        '--targets', 'peaks', 'events',
        '--output_folder', '/tmp/output',
        '--logs_path', '/tmp/logs',
        '--dry_run',
    ]
    
    with patch('sys.argv', ['offline_processing.py'] + args):
        parsed_args = parse_args()
        main(parsed_args)
    
    # Assert submit_job was called (since --dry_run is used, it should not actually execute)
    assert mock_submit_job.called
    assert mock_submit_job.call_count == 1
    job_string = mock_submit_job.call_args[1]['jobstring']
    print(job_string)
    assert "Processing run 000123" in job_string

def test_parse_args():
    args = [
        '--run_id', '123', '124',
        '--targets', 'peaks', 'events',
        '--output_folder', '/tmp/output',
        '--logs_path', '/tmp/logs',
        '--dry_run',
    ]
    
    with patch('sys.argv', ['offline_processing.py'] + args):
        parsed_args = parse_args()
        assert parsed_args.run_id == ['123', '124']
        assert parsed_args.targets == ['peaks', 'events']
        assert parsed_args.output_folder == '/tmp/output'
        assert parsed_args.logs_path == '/tmp/logs'
        assert parsed_args.dry_run
