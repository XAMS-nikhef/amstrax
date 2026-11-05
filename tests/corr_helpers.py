"""Correction inputs for the event-structure tests (sr_corrections / sr_maps of mode 1)."""
import numpy as np

EDGES = list(np.arange(-32, 33, 4.0))
NB = len(EDGES) - 1


def _maps(s1=1.0, s2=1.0, q=(0.0, 0.0, 1.0)):
    s1m = [[s1] * NB for _ in range(NB)]
    s2m = [[s2] * NB for _ in range(NB)]
    s2m[0][0] = None                                       # a cell without a value
    return {"edges_mm": EDGES, "s1_rel": s1m, "s2_top_rel": s2m,
            "s1_z": {"quadratic_in_drift_us": list(q), "t_norm_us": 21.0, "ref_v_drift_mm_per_us": 1.325, "ref_t_gate_us": 1.45}}


def _corr(tau=None, calibrated=True):
    return {"tau": tau or {"fixed_us": 500.0}, "g1": 0.0568, "g2_top": 0.332, "energy_calibrated": calibrated}
