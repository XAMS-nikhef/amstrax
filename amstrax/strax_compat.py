"""Compatibility with strax 1.x and 2.x for the peak-building plugins.

strax 2 changed two things the amstrax peak plugins rely on:

* ``strax.peak_dtype`` always adds ``area_fraction_top``, ``center_time``, ``median_time`` (and, with hit timing,
  ``first_channel`` / ``last_channel``), and ``strax.split_peaks`` needs these fields internally.
* ``strax.compute_widths`` returns ``(median_time, width, area_decile_from_midpoint)`` instead of filling the peaks.

The plugins therefore build peaks with the native ``strax.peak_dtype`` and store them in the strax-1.5 layout
(``legacy_peak_dtype``), so the stored peaks keep the same format and lineage under both strax versions.
"""
import numpy as np
import strax

export, __all__ = strax.exporter()


@export
def legacy_peak_dtype(n_channels=100, n_sum_wv_samples=200, n_widths=11, digitize_top=True, hits_timing=True):
    """Peak dtype exactly as ``strax.peak_dtype`` in strax 1.5.4 (layout of all peaks stored so far)."""
    if n_channels == 1:
        raise ValueError("Must have more than one channel")
    dtype = strax.peak_interval_dtype + [   # identical in strax 1.5.4 and 2.2.3 (dt as int32)
        (("Classification of the peak(let)", "type"), np.int8),
        (("Integral across channels [PE]", "area"), np.float32),
        (("Integral per channel [PE]", "area_per_channel"), np.float32, n_channels),
        (("Number of hits contributing at least one sample to the peak ", "n_hits"), np.int32),
        (("Waveform data in PE/sample (not PE/ns!)", "data"), np.float32, n_sum_wv_samples),
        (("Peak widths in range of central area fraction [ns]", "width"), np.float32, n_widths),
        (("Peak widths: time between nth and 5th area decile [ns]", "area_decile_from_midpoint"), np.float32, n_widths),
        (("Does the channel reach ADC saturation?", "saturated_channel"), np.int8, n_channels),
        (("Total number of saturated channels", "n_saturated_channels"), np.int16),
        (("Channel within tight range of mean", "tight_coincidence"), np.int16),
        (("Largest gap between hits inside peak [ns]", "max_gap"), np.int32),
        (("Maximum interior goodness of split", "max_goodness_of_split"), np.float32),
    ]
    if hits_timing:
        dtype += [
            (("Largest time difference between apexes of hits inside peak [ns]", "max_diff"), np.int32),
            (("Smallest time difference between apexes of hits inside peak [ns]", "min_diff"), np.int32),
        ]
    if digitize_top:
        dtype.insert(9, (("Waveform data in PE/sample (not PE/ns!), top array", "data_top"), np.float32, n_sum_wv_samples))
    return dtype


@export
def compute_peak_widths(peaks):
    """Fill ``width`` and ``area_decile_from_midpoint`` (and ``median_time`` if present) in place, for strax 1 and 2."""
    if not len(peaks):
        return
    out = strax.compute_widths(peaks)
    if out is None:                       # strax 1.x: filled in place
        return
    median_time, width, area_decile_from_midpoint = out
    peaks["width"] = width
    peaks["area_decile_from_midpoint"] = area_decile_from_midpoint
    if "median_time" in peaks.dtype.names:
        peaks["median_time"] = median_time


@export
def to_dtype(arr, dtype):
    """Copy the fields of ``dtype`` from ``arr`` into a new array of that dtype (extra fields of ``arr`` are dropped)."""
    out = np.zeros(len(arr), dtype=dtype)
    for name in out.dtype.names:
        out[name] = arr[name]
    return out
