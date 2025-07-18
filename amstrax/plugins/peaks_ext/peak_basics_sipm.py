import numba
import numpy as np
import strax
from immutabledict import immutabledict

export, __all__ = strax.exporter()


# For n_competing, which is temporarily added to PeakBasics
@export
@strax.takes_config(
    strax.Option(
        "channel_map",
        type=immutabledict,
        track=False,
        help="Map of channel numbers to top, bottom and aqmon, to be defined in the context",
    ),
    strax.Option(
        "check_peak_sum_area_rtol",
        default=1e-4,
        help="Check if the area of the sum-wf is the same as the total area"
        " (if the area of the peak is positively defined)."
        " Set to None to disable.",
    ),
    strax.Option(
      's1_min_width', 
      default=10,
      help="Minimum (IQR) width of S1s"
    ),
    strax.Option(
      's1_max_width',
      default=225,
      help="Maximum (IQR) width of S1s"
    ),
    strax.Option(
      's1_min_area',
      default=10,
      help="Minimum area (PE) for S1s"
    ),
    strax.Option(
      's2_min_area',
      default=10,
      help="Minimum area (PE) for S2s"
    ),
    strax.Option(
      's2_min_width',
      default=225,
      help="Minimum width for S2s"
    ),
    strax.Option(
      's1_min_channels',
      default=5,
      help="Minimum number of channels for S1s"
    ),
    strax.Option(
      's2_min_channels',
      default=5,
      help="Minimum number of channels for S2s"
    ),
    strax.Option(
      's2_min_area_fraction_top',
      default=0,
      help="Minimum area fraction top for S2s"
    ),
    strax.Option(
      's1_max_area_fraction_top',
      default=.2,
      help="Maximum area fraction top for S1s"
    ),
)
class PeakBasicsSiPM(strax.Plugin):
    provides = ("peak_basics_sipm",)
    depends_on = ("peaks_sipm", )
    data_kind = "peaks_sipm"

    parallel = "False"
    rechunk_on_save = False
    __version__ = "2.1"
    dtype = [
        (('Start time of the peak (ns since unix epoch)',
          'time'), np.int64),
        (('End time of the peak (ns since unix epoch)',
          'endtime'), np.int64),
        (('Weighted center time of the peak (ns since unix epoch)',
          'center_time'), np.int64),
        (('Peak integral in PE',
          'area'), np.float32),
        (('Number of hits contributing at least one sample to the peak',
          'n_hits'), np.int32),
        (('Number of PMTs contributing to the peak',
          'n_channels'), np.int16),
        (('PMT number which contributes the most PE',
          'max_pmt'), np.int16),
        (('Area of signal in the largest-contributing PMT (PE)',
          'max_pmt_area'), np.float32),
        (('Total number of saturated channels',
          'n_saturated_channels'), np.int16),
        (('Width (in ns) of the central 50% area of the peak',
          'range_50p_area'), np.float32),
        (('Width (in ns) of the central 90% area of the peak',
          'range_90p_area'), np.float32),
        (('Fraction of area seen by the top array '
          '(NaN for peaks with non-positive area)',
          'area_fraction_top'), np.float32),
        (('Length of the peak waveform in samples',
          'length'), np.int32),
        (('Time resolution of the peak waveform in ns',
          'dt'), np.int16),
        (('Time between 10% and 50% area quantiles [ns]',
          'rise_time'), np.float32),
        (('Number of PMTs with hits within tight range of mean',
          'tight_coincidence'), np.int16),
        (('Type of peak (s1 or s2)',
          'type'), np.int16),
    ]

    def compute(self, peaks_sipm):
        p = peaks_sipm
        r = np.zeros(len(p), self.dtype)
        needed_fields = 'time length dt area type'
        for q in needed_fields.split():
            r[q] = p[q]

        r['endtime'] = p['time'] + p['dt'] * p['length']
        r['n_channels'] = (p['area_per_channel'] > 0).sum(axis=1)
        r['n_hits'] = p['n_hits']
        r['range_50p_area'] = p['width'][:, 5]
        r['range_90p_area'] = p['width'][:, 9]
        r['max_pmt'] = np.argmax(p['area_per_channel'], axis=1)
        r['max_pmt_area'] = np.max(p['area_per_channel'], axis=1)
        r['tight_coincidence'] = p['tight_coincidence']
        r['n_saturated_channels'] = p['n_saturated_channels']

        # Negative-area peaks get NaN AFT
        m = p['area'] > 0
        r['rise_time'] = -p['area_decile_from_midpoint'][:, 1]

        # Negative or zero-area peaks have centertime at startime
        r["center_time"] = p["time"]
        r["center_time"][m] += self.compute_center_times(p[m])

        # Determine peak type
        # 0 = unknown
        # 1 = s1
        # 2 = s2
                
        is_s1 = p['area'] >= 0
        is_s1 &= r['range_50p_area'] > self.config['s1_min_width']
        is_s1 &= r['range_50p_area'] < self.config['s1_max_width']
        is_s1 &= r['n_channels'] >= 0
        
        is_s2 = p['area'] > 0
        is_s2 &= r['range_50p_area'] > self.config['s2_min_width']
        is_s2 &= r['n_channels'] >= 0
        
        # if both are true, then it's an unknown peak
        is_s1 &= ~is_s2
        is_s2 &= ~is_s1

        r['type'][is_s1] = 1
        r['type'][is_s2] = 2

        return r


    @staticmethod
    @numba.jit(nopython=True, nogil=True, cache=False)
    def find_n_competing(peaks, window, fraction):
        n = len(peaks)
        t = peaks["time"]
        a = peaks["area"]
        results = np.zeros(n, dtype=np.int16)
        left_i = 0
        right_i = 0
        for i, peak in enumerate(peaks):
            while t[left_i] + window < t[i] and left_i < n - 1:
                left_i += 1
            while t[right_i] - window < t[i] and right_i < n - 1:
                right_i += 1
            results[i] = np.sum(a[left_i : right_i + 1] > a[i] * fraction)

        return results

    @staticmethod
    @numba.njit(cache=True, nogil=True)
    def compute_center_times(peaks):
        result = np.zeros(len(peaks), dtype=np.int32)
        for p_i, p in enumerate(peaks):
            t = 0
            for t_i, weight in enumerate(p["data"]):
                t += t_i * p["dt"] * weight
            result[p_i] = t / p["area"]
        return result

    @staticmethod
    def check_area(area_per_channel_sum, peaks, rtol) -> None:
        """
        Check if the area of the sum-wf is the same as the total area
            (if the area of the peak is positively defined).

        :param area_per_channel_sum: the summation of the
            peaks['area_per_channel'] which will be checked against the
             values of peaks['area'].
        :param peaks: array of peaks.
        :param rtol: relative tolerance for difference between
            area_per_channel_sum and peaks['area']. See np.isclose.
        :raises: ValueError if the peak area and the area-per-channel
            sum are not sufficiently close
        """
        positive_area = peaks["area"] > 0
        if not np.sum(positive_area):
            return

        is_close = np.isclose(
            area_per_channel_sum[positive_area],
            peaks[positive_area]["area"],
            rtol=rtol,
        )

        if not is_close.all():
            for peak in peaks[positive_area][~is_close]:
                print("bad area")
                strax.print_record(peak)

            p_i = np.where(~is_close)[0][0]
            peak = peaks[positive_area][p_i]
            area_fraction_off = (
                1 - area_per_channel_sum[positive_area][p_i] / peak["area"]
            )
            message = (
                f"Area not calculated correctly, it's "
                f'{100 * area_fraction_off} % off, time: {peak["time"]}'
            )
            raise ValueError(message)
