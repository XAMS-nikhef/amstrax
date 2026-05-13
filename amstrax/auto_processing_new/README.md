# XAMS STBC Auto-Processing (Current)

This folder is the current production path for XAMS processing on STBC.

## Scripts

- `auto_processing.py`
  - Online loop: watches run DB and submits Condor jobs.
  - Typical use: running in `screen` during DAQ operations.
- `process.py`
  - Worker script executed inside each Condor job.
  - Processes one run for one or more targets and updates rundoc data entries.
- `offline_processing.py`
  - Bulk reprocessing submitter for explicit run lists/ranges/files.
  - Typical use: offline campaigns with explicit amstrax/corrections settings.
- `db_utils.py`
  - RunDB helper methods used by the scripts above.
- `job_submission.py`
  - Condor submission wrapper utilities.

## Environment prerequisites

- Use XAMS environment:
  - `source /data/xenon/xams_v2/setup.sh`
- Keep configuration in `~/.xams_config` for paths and DB settings.
- Production writes should be done as user `xamsdata`.

## Operational flows

### 1) Online loop (continuous)

```bash
cd /data/xenon/xams_v2/software/amstrax/amstrax/auto_processing_new
python auto_processing.py \
  --target raw_records peak_basics event_basics event_positions event_info \
  --production \
  --max_jobs 8 \
  --queue short \
  --mem 8000
```

Run in screen:

```bash
screen -S xams_auto_processing_online
# run command above
```

### 2) Single-run submit via online submitter

```bash
python auto_processing.py \
  --run_id 007348 \
  --target peak_basics event_basics event_positions event_info \
  --corrections_version ONLINE \
  --production
```

### 3) Offline bulk reprocessing

```bash
python offline_processing.py \
  --run_range 7340-7350 \
  --targets peak_basics event_basics event_positions event_info \
  --amstrax_path /data/xenon/xams_v2/software/amstrax_versioned/v2.1.0 \
  --corrections_version ONLINE \
  --production \
  --queue short \
  --mem 8000
```

## Notes

- `corrections_version` must cover the run ranges used by each referenced correction file.
- For LED calibration modes, `process.py` automatically switches to:
  - `raw_records`, `records_led`, `led_calibration`
- `process.py` now accepts `--set_config_kwargs` and `--set_context_kwargs` for compatibility.

