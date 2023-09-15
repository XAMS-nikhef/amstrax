import numba
import numpy as np
import strax

export, __all__ = strax.exporter()


# For n_competing, which is temporarily added to PeakBasics
@export
@strax.takes_config(
    strax.Option(
        "min_area_fraction",
        default=0.5,
        help="The area of competing peaks must be at least "
        "this fraction of that of the considered peak",
    ),
    strax.Option(
        "nearby_window",
        default=int(1e6),
        help="Peaks starting within this time window (on either side)"
        "in ns count as nearby.",
    ),
    strax.Option(
        "n_top_pmts",
        default=4,
        help="Number of top PMTs to consider for area fraction top",
    ),
    strax.Option(
        "check_peak_sum_area_rtol",
        default=1e-4,
        help="Check if the area of the sum-wf is the same as the total area"
        " (if the area of the peak is positively defined)."
        " Set to None to disable.",
    ),
)
class PeakBasics(strax.Plugin):
    provides = ("peak_basics",)
    depends_on = "peaks"
    data_kind = "peaks"

    parallel = "False"
    rechunk_on_save = False
    __version__ = "1.0"
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
        (('Classification of the peak(let)',
          'type'), np.int8),
    ]


    def compute(self, peaks):
        p = peaks
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

        n_top = self.config["n_top_pmts"]
        area_top = p['area_per_channel'][:, :n_top].sum(axis=1)
        # Recalculate to prevent numerical inaccuracy #442
        area_total = p['area_per_channel'].sum(axis=1)
        # Negative-area peaks get NaN AFT
        m = p['area'] > 0
        r['area_fraction_top'][m] = area_top[m] / area_total[m]
        r['area_fraction_top'][~m] = float('nan')
        r['rise_time'] = -p['area_decile_from_midpoint'][:, 1]

        if self.config['check_peak_sum_area_rtol'] is not None:
            self.check_area(area_total, p, self.config['check_peak_sum_area_rtol'])
        # Negative or zero-area peaks have centertime at startime
        r['center_time'] = p['time']
        r['center_time'][m] += self.compute_center_times(peaks[m])
        return r


        p = peaks
        p = strax.sort_by_time(p)
        r = np.zeros(len(p), self.dtype)
        for q in "time length dt area".split():
            r[q] = p[q]
        r["endtime"] = p["time"] + p["dt"] * p["length"]
        r["n_channels"] = (p["area_per_channel"] > 0).sum(axis=1)
        r["range_50p_area"] = p["width"][:, 5]
        r["max_pmt"] = np.argmax(p["area_per_channel"], axis=1)
        r["max_pmt_area"] = np.max(p["area_per_channel"], axis=1)

        # area_top = p['area_per_channel'][:, :8].sum(axis=1)
        area_top = p["area_per_channel"][:, 1:2].sum(axis=1)  # top pmt in ch 1
        # Negative-area peaks get 0 AFT - TODO why not NaN?
        m = p["area"] > 0
        r["area_fraction_top"][m] = area_top[m] / p["area"][m]
        # n_competing temporarily due to chunking issues
        r["n_competing"] = self.find_n_competing(
            peaks,
            window=self.config["nearby_window"],
            fraction=self.config["min_area_fraction"],
        )

        area_total = p['area_per_channel'].sum(axis=1)
        check_peak_sum_area_rtol = 1e-4
        if check_peak_sum_area_rtol is not None:
            self.check_area(area_total, p, self.check_peak_sum_area_rtol)
        # Negative or zero-area peaks have centertime at startime
        r["center_time"] = p["time"]
        r["center_time"][m] += self.compute_center_times(peaks[m])

        return r

    # n_competing
    def get_window_size(self):
        return 2 * self.config["nearby_window"]

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
