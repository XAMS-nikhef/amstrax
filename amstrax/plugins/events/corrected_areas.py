from typing import Tuple

import numpy as np
import strax
import amstrax

export, __all__ = strax.exporter()


@export
class CorrectedAreas(strax.Plugin):
    """Plugin which applies light collection efficiency maps and electron life time to the data.

    Computes the cS1/cS2 for the main/alternative S1/S2 as well as the
    corrected life time.
    Note:
        Please be aware that for both, the main and alternative S1, the
        area is corrected according to the xy-position of the main S2.
        There are now 3 components of cS2s: cs2_top, cS2_bottom and cs2.
        cs2_top and cs2_bottom are corrected by the corresponding maps,
        and cs2 is the sum of the two.

    """

    __version__ = "0.5.1"

    depends_on = ("event_basics", "event_positions")

    elife = amstrax.XAMSConfig(default=30000, help="electron lifetime in [ns]")

    config_bla = amstrax.XAMSConfig(default=28, help="bla bla bla")

    def infer_dtype(self):
        dtype = []
        dtype += strax.time_fields

        for peak_type, peak_name in zip(["", "alt_"], ["main", "alternate"]):
            # Only apply
            dtype += [
                (f"{peak_type}cs1", np.float32, f"Corrected area of {peak_name} S1 [PE]"),
            ]
            dtype += [
                (f"{peak_type}cs2", np.float32, f"Corrected area of {peak_name} S2 [PE]"),
            ]

        return dtype

    def compute(self, events):
        result = np.zeros(len(events), self.dtype)
        result["time"] = events["time"]
        result["endtime"] = events["endtime"]

        # S1 corrections depend on the actual corrected event position.
        # We use this also for the alternate S1; for e.g. Kr this is
        # fine as the S1 correction varies slowly.
        # event_positions = np.vstack([events["x"], events["y"], events["z"]]).T

        elife = self.config["elife"]

        for peak_type in ["", "alt_"]:

            result[f"{peak_type}cs1"] = events[f"{peak_type}s1_area"]
            result[f"{peak_type}cs2"] = events[f"{peak_type}s2_area"] * np.exp(events["drift_time"] / elife)

        return result
