from typing import Tuple

import numpy as np
import strax
import amstrax

export, __all__ = strax.exporter()

@export
@strax.takes_config(
    strax.Option(
        "elife",
        default=30000,
        help="electron lifetime in [ns] (should be implemented in db soon)",
    ),
    strax.Option(
        "s1_naive_z_correction",
        default=[-52, 0, 700, -7.69],
        help="Parameters for the z-dependent S1 correction \
            [zmin, zmax, y0, a] where y0 + a*z is the correction",
    )
)
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

    __version__ = "0.6.0"

    depends_on = ("event_basics", "event_positions")

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


    def s1_naive_z_correction(self, z):
        """
        Apply a naive z-dependent S1 correction.
        Returns the correction factor for the S1 area.
        """

        s1_correction_function = lambda z: y0 + a * z
        zmin, zmax, y0, a = self.config["s1_naive_z_correction"]
        s1_correction_average = s1_correction_function((zmin + zmax) / 2)
        correction = s1_correction_average/s1_correction_function(z)

        return correction

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

            result[f"{peak_type}cs1"] = events[f"{peak_type}s1_area"]*self.s1_naive_z_correction(events["z"])
            result[f"{peak_type}cs2"] = events[f"{peak_type}s2_area"]*np.exp(events["drift_time"]/elife)

        return result
