"""amstrax.strax_compat: peaks keep the strax-1.5 layout under strax 1 and 2."""
import numpy as np
import strax
import amstrax


def test_legacy_peak_dtype_matches_strax_1():
    if int(strax.__version__.split(".")[0]) >= 2:
        return                                  # only strax 1.x still provides this layout itself
    for n in (2, 5, 8):
        assert np.dtype(amstrax.legacy_peak_dtype(n_channels=n)) == np.dtype(strax.peak_dtype(n_channels=n))


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
