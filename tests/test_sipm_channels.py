"""SiPM channels: the context does not force a channel map, and peaks_sipm keeps the area of every SiPM channel."""
import numpy as np
import strax
from immutabledict import immutabledict

import amstrax

TWO_SIPMS = immutabledict(bottom=(0, 0), top=(1, 4), external=(5, 5), sipm=(6, 7), aqmon=(40, 40))


def _context():
    st = amstrax.contexts.xams(init_rundb=False)
    st.set_config(dict(channel_map=TWO_SIPMS, gain_to_pe_array=np.ones(8)))
    return st


def test_context_does_not_set_channel_map():
    # a channel_map in the context would override the per-run map of every plugin except the DAQReader;
    # only an offline context (no run database) gets the default map
    assert "channel_map" not in amstrax.contexts.XAMS_COMMON_CONFIG
    st = amstrax.contexts.xams(init_rundb=False)
    assert dict(st.config["channel_map"]) == dict(amstrax.xams_config.DEFAULT_CHANNEL_MAP)


def _records(channels, area_adc=200, length=40):
    r = np.zeros(len(channels), dtype=strax.record_dtype())
    for i, ch in enumerate(channels):
        r[i]["time"] = 1000 + 10_000 * i            # separate peaks, 10 us apart
        r[i]["dt"], r[i]["length"], r[i]["pulse_length"] = 4, length, length
        r[i]["channel"] = ch
        r[i]["data"][:length] = 0
        r[i]["data"][10:20] = area_adc // 10
    return r


def test_peaks_sipm_keeps_both_sipm_channels():
    st = _context()
    p = st.get_single_plugin("000001", "peaks_sipm")
    assert p.dtype["area_per_channel"].shape == (8,)   # channels 0..7
    peaks = p.compute(_records([6, 7, 6, 7]), start=0, end=100_000)
    per_ch = peaks["area_per_channel"].sum(axis=0)
    assert per_ch[6] > 0 and per_ch[7] > 0
    assert np.isclose(per_ch[6], per_ch[7])
    np.testing.assert_allclose(peaks["area_per_channel"].sum(axis=1), peaks["area"], rtol=1e-5)


def test_peaks_sipm_rejects_channels_beyond_max():
    st = _context()
    st.set_config(dict(channel_map=immutabledict(sipm=(6, 9))))
    try:
        st.get_single_plugin("000001", "peaks_sipm")
    except ValueError as e:
        assert "max_sipm_channel" in str(e)
    else:
        raise AssertionError("expected a ValueError for SiPM channels above max_sipm_channel")
