Auto Processing On STBC (Current)
=================================

Scope
-----

This page documents the current XAMS production processing flow on STBC.
Use this as the operational reference.

Code location
-------------

Current scripts live in:

.. code-block:: bash

    amstrax/auto_processing_new/

Main entry points:

- ``auto_processing.py``: online loop + single-run submit
- ``process.py``: per-run worker executed in Condor jobs
- ``offline_processing.py``: explicit bulk offline reprocessing

Environment
-----------

.. code-block:: bash

    source /data/xenon/xams_v2/setup.sh

Also ensure ``~/.xams_config`` is configured for paths/DB access.

Production writes should run as user ``xamsdata``.

Online processing loop
----------------------

Typical start command:

.. code-block:: bash

    cd /data/xenon/xams_v2/software/amstrax/amstrax/auto_processing_new
    python auto_processing.py \
      --target raw_records peak_basics event_basics event_positions event_info \
      --production \
      --max_jobs 8 \
      --queue short \
      --mem 8000

Recommended in a screen session:

.. code-block:: bash

    screen -S xams_auto_processing_online
    # run command above

Single-run submit
-----------------

.. code-block:: bash

    python auto_processing.py \
      --run_id 007348 \
      --target peak_basics event_basics event_positions event_info \
      --corrections_version ONLINE \
      --production

Offline bulk reprocessing
-------------------------

.. code-block:: bash

    python offline_processing.py \
      --run_range 7340-7350 \
      --targets peak_basics event_basics event_positions event_info \
      --amstrax_path /data/xenon/xams_v2/software/amstrax_versioned/v2.1.0 \
      --corrections_version ONLINE \
      --production \
      --queue short \
      --mem 8000

Operational notes
-----------------

- ``corrections_version`` must cover the run ranges in each referenced correction file.
- LED calibration runs are auto-routed by ``process.py`` to:
  ``raw_records``, ``records_led``, ``led_calibration``.
- ``process.py`` supports optional JSON kwargs:
  ``--set_config_kwargs`` and ``--set_context_kwargs``.

