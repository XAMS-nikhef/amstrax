import numba
import numpy as np
import strax
export, __all__ = strax.exporter()

@export
class EventInfo(strax.MergeOnlyPlugin):
    depends_on = ['event_basics',
                  'corrected_areas',
                  'event_positions',
                  # 'energy_estimates',
                  ]
    rechunk_on_save = True
    provides = 'event_info'
    save_when = strax.SaveWhen.ALWAYS
    __version__ = '0.0.2'
