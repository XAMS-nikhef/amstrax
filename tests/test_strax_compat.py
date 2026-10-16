"""amstrax.strax_compat: peaks keep the strax-1.5 layout under strax 1 and 2."""
import numpy as np
import strax
import amstrax


def test_legacy_peak_dtype_matches_strax_1_5_4():
    if strax.__version__ != "1.5.4":
        return                                  # the stored layout is that of strax 1.5.4 (production)
    for n in (2, 5, 8):
        assert np.dtype(amstrax.legacy_peak_dtype(n_channels=n)) == np.dtype(strax.peak_dtype(n_channels=n))


def test_legacy_peak_dtype_fields():
    names = np.dtype(amstrax.legacy_peak_dtype(n_channels=5)).names
    for f in ("time", "length", "dt", "channel", "type", "area", "area_per_channel", "n_hits", "data", "data_top",
              "width", "area_decile_from_midpoint", "saturated_channel", "n_saturated_channels", "tight_coincidence",
              "max_gap", "max_goodness_of_split", "max_diff", "min_diff"):
        assert f in names


def test_widths_and_copy_to_legacy_layout():
    native = np.zeros(3, dtype=strax.peak_dtype(n_channels=5))
    native["dt"], native["length"] = 10, 20
    native["data"][:, :20] = np.hanning(20)
    native["area"] = native["data"].sum(axis=1)
    amstrax.compute_peak_widths(native)
    assert np.all(native["width"][:, 5] > 0)    # 50 % width filled in place under strax 1 and 2
    legacy = amstrax.to_dtype(native, amstrax.legacy_peak_dtype(n_channels=5))
    assert legacy.dtype == np.dtype(amstrax.legacy_peak_dtype(n_channels=5))
    np.testing.assert_array_equal(legacy["width"], native["width"])
    amstrax.compute_peak_widths(native[:0])     # empty input is fine
