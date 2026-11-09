=======
amstrax
=======

``amstrax`` processes the data of XAMS, the dual-phase xenon TPC at Nikhef, on top of
`strax <https://github.com/AxFoundation/strax>`_: from the redax chunks of the digitizer to pulses, peaks and the
event tables. Code: https://github.com/XAMS-nikhef/amstrax.

Where to find what
==================

- **This site**: the code reference, generated from the source at every change of master: every data type with its
  columns, dependencies and options (:doc:`reference/datastructure_xams`), and the API (:doc:`reference/amstrax`).
- **XAMS handbook** (https://www.nikhef.nl/~xamsdata/, Nikhef login): the canonical documentation — processing on
  STBC (online and offline), corrections versions, the event data format with worked examples and the old format,
  and how to start an analysis.
- **Overview note** `XAMS_amstrax_overview.pdf
  <https://github.com/XAMS-nikhef/amstrax/blob/master/docs/notes/XAMS_amstrax_overview.pdf>`_: where every plugin
  enters the reconstruction stream, where the settings come from, the event tables and the map from the old
  ``event_info`` columns.

Quick start
===========

On STBC (``source /data/xenon/xams_v2/setup.sh``):

.. code-block:: python

    import amstrax

    st = amstrax.contexts.xams(corrections_version="PROD5", output_folder="./strax_data")
    run = "007790"
    assert st.is_stored(run, "events")                   # do not trigger processing by accident
    ev = st.get_df(run, ["events", "event_cuts"])        # one row per event, with the cut booleans
    m = amstrax.main_interaction(st, run)                # main S1, main site and its S2 in one row per event

.. toctree::
    :maxdepth: 1
    :caption: Reference

    reference/datastructure_xams
    reference/amstrax

.. toctree::
    :maxdepth: 1
    :caption: Maintaining amstrax

    writing_documentation
    making_a_release

Indices
=======

* :ref:`genindex`
* :ref:`modindex`
