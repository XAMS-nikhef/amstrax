import numba
import numpy as np
import strax
from immutabledict import immutabledict

import amstrax

export, __all__ = strax.exporter()
__all__ += ['NO_PULSE_COUNTS']

HITFINDER_OPTIONS = tuple([
    strax.Option(
        'hit_min_amplitude',
        default='xams_thresholds',
        help='Minimum hit amplitude in ADC counts above baseline. '
             'See amstrax.hit_min_amplitude in hitfinder_thresholds.py for options.'
    )])


@export
@strax.takes_config(
    strax.Option(
        'baseline_samples',
        default=40, infer_type=False,
        help='Number of samples to use at the start of the pulse to determine '
             'the baseline'),
    # PMT pulse processing options
    strax.Option(
        'pmt_pulse_filter',
        default=None, infer_type=False,
        help='Linear filter to apply to pulses, will be normalized.'),   
    strax.Option(
        'save_outside_hits',
        default=(3, 20), infer_type=False,
        help='Save (left, right) samples besides hits; cut the rest'),
    strax.Option(
        'n_tpc_pmts', type=int,
        help='Number of TPC channels'),
    strax.Option(
        'check_raw_record_overlaps',
        default=True, track=False, infer_type=False,
        help='Crash if any of the pulses in raw_records overlap with others '
             'in the same channel'),
    strax.Option(
        'allow_sloppy_chunking',
        default=True, track=False, infer_type=False,
        help=('Use a default baseline for incorrectly chunked fragments. '
              'This is a kludge for improperly converted XENON1T data.')),
    *HITFINDER_OPTIONS)
class PulseProcessing(strax.Plugin):
    """
    Get the specific raw_records of the measurements 
    (raw_records_v1724 or raw_records_v1730)
    and split the raw_records into:
    - records
    - pulse_counts
    Apply basic pulse processing:
        1. Flip the pulse if it is necessary (only for the PMT pulse)
        2. Calculate the baseline and integrate the waveform
        3. Find hits
        4. Filter the record and cut outside the hit bounds

    pulse_counts holds some average information for the individual PMT
    channels for each chunk of raw_records. This includes e.g.
    number of recorded pulses, lone_pulses (pulses which do not
    overlap with any other pulse), or mean values of baseline and
    baseline rms channel.
    """
    __version__ = '0.2.72'
    
    parallel = 'process'
    rechunk_on_save = immutabledict(
        records=False,
        pulse_counts=True)
    compressor = 'zstd'

    depends_on = ('raw_records')

    provides = ('records', 'pulse_counts')
    data_kind = {k: k for k in provides}
    save_when = strax.SaveWhen.TARGET
       
    def infer_dtype(self,):
        # The record_length is the same for both raw_records_v1724 and raw_records_v1730
        # therefore, we can just use one of the two
        self.record_length = strax.record_length_from_dtype(
            self.deps['raw_records'].dtype_for('raw_records'))

        dtype = dict()
        for p in self.provides:
            if 'records' in p:
                dtype[p] = strax.record_dtype(self.record_length)
        dtype['pulse_counts'] = pulse_count_dtype(self.config['n_tpc_pmts'])
        ntpc = self.config['n_tpc_pmts']
        return dtype

    def compute(self, raw_records, start, end):  

        if self.config['check_raw_record_overlaps']:
            check_overlaps(raw_records, n_channels=3000)

        # Throw away any non-TPC records; this should only happen for XENON1T
        # converted data
        raw_records = raw_records[
            raw_records['channel'] < self.config['n_tpc_pmts']]


        # Convert everything to the records data type -- adds extra fields.
        r = strax.raw_to_records(raw_records)
        del raw_records
        
        # Do not trust in DAQ + strax.baseline to leave the
        # out-of-bounds samples to zero.
        # FIXME: better to throw an error if something is nonzero
        strax.zero_out_of_bounds(r)

        baseline_per_channel(r, baseline_samples=self.config['baseline_samples'],
                           allow_sloppy_chunking=self.config['allow_sloppy_chunking'],
                             flip=True)
             
        strax.integrate(r)       
        
        pulse_counts = count_pulses(r, self.config['n_tpc_pmts'])
        pulse_counts['time'] = start
        pulse_counts['endtime'] = end

        # #For now left out, to look at in the future 
        #
        # if len(r):
        #     # Find hits
        #     # -- before filtering,since this messes with the with the S/N
        #     hits = strax.find_hits(
        #         r, min_amplitude=amstrax.hit_min_amplitude(
        #             self.config['hit_min_amplitude']))

        #     if self.config['pmt_pulse_filter']:
        #         # Filter to concentrate the PMT pulses
        #         strax.filter_records(
        #             r, np.array(self.config['pmt_pulse_filter']))

        #     le, re = self.config['save_outside_hits']
        #     r = strax.cut_outside_hits(r, hits,
        #                                left_extension=le,
        #                                right_extension=re)

        #     # Probably overkill, but just to be sure...
        #     strax.zero_out_of_bounds(r)

        return dict(records=r,
                    pulse_counts=pulse_counts)

##
# Pulse counting
##
@export
def pulse_count_dtype(n_channels):
    # NB: don't use the dt/length interval dtype, integer types are too small
    # to contain these huge chunk-wide intervals
    return [
        (('Start time of the chunk', 'time'), np.int64),
        (('End time of the chunk', 'endtime'), np.int64),
        (('Number of pulses', 'pulse_count'),
         (np.int64, n_channels)),
        (('Number of lone pulses', 'lone_pulse_count'),
         (np.int64, n_channels)),
        (('Integral of all pulses in ADC_count x samples', 'pulse_area'),
         (np.int64, n_channels)),
        (('Integral of lone pulses in ADC_count x samples', 'lone_pulse_area'),
         (np.int64, n_channels)),
        (('Average baseline', 'baseline_mean'),
         (np.int16, n_channels)),
        (('Average baseline rms', 'baseline_rms_mean'),
         (np.float32, n_channels)),
    ]


def count_pulses(records, n_channels):
    """Return array with one element, with pulse count info from records"""
    if len(records):
        result = np.zeros(1, dtype=pulse_count_dtype(n_channels))
        _count_pulses(records, n_channels, result)
        return result
    return np.zeros(0, dtype=pulse_count_dtype(n_channels))


NO_PULSE_COUNTS = -9999  # Special value required by average_baseline in case counts = 0


@numba.njit(cache=True, nogil=True)
def _count_pulses(records, n_channels, result):
    count = np.zeros(n_channels, dtype=np.int64)
    lone_count = np.zeros(n_channels, dtype=np.int64)
    area = np.zeros(n_channels, dtype=np.int64)
    lone_area = np.zeros(n_channels, dtype=np.int64)

    last_end_seen = 0
    next_start = 0

    # Array of booleans to track whether we are currently in a lone pulse
    # in each channel
    in_lone_pulse = np.zeros(n_channels, dtype=np.bool_)
    baseline_buffer = np.zeros(n_channels, dtype=np.float64)
    baseline_rms_buffer = np.zeros(n_channels, dtype=np.float64)
    for r_i, r in enumerate(records):
        if r_i != len(records) - 1:
            next_start = records[r_i + 1]['time']

        ch = r['channel']
        if ch >= n_channels:
            print('Channel:', ch)
            raise RuntimeError("Out of bounds channel in get_counts!")

        area[ch] += r['area']  # <-- Summing total area in channel

        if r['record_i'] == 0:
            count[ch] += 1
            baseline_buffer[ch] += r['baseline']
            baseline_rms_buffer[ch] += r['baseline_rms']

            if (r['time'] > last_end_seen
                    and r['time'] + r['pulse_length'] * r['dt'] < next_start):
                # This is a lone pulse
                lone_count[ch] += 1
                in_lone_pulse[ch] = True
                lone_area[ch] += r['area']
            else:
                in_lone_pulse[ch] = False

            last_end_seen = max(last_end_seen,
                                r['time'] + r['pulse_length'] * r['dt'])

        elif in_lone_pulse[ch]:
            # This is a subsequent fragment of a lone pulse
            lone_area[ch] += r['area']

    res = result[0]  # Supposed to be [0] ??
    res['pulse_count'][:] = count[:]
    res['lone_pulse_count'][:] = lone_count[:]
    res['pulse_area'][:] = area[:]
    res['lone_pulse_area'][:] = lone_area[:]
    means = (baseline_buffer / count)
    means[np.isnan(means)] = NO_PULSE_COUNTS
    res['baseline_mean'][:] = means[:]
    res['baseline_rms_mean'][:] = (baseline_rms_buffer / count)[:]


##
# Misc
##
@export
@numba.njit(cache=True, nogil=True)
def mask_and_not(x, mask):
    return x[mask], x[~mask]


@export
@numba.njit(cache=True, nogil=True)
def channel_split(rr, first_other_ch):
    """Return """
    return mask_and_not(rr, rr['channel'] < first_other_ch)


@export
def check_overlaps(records, n_channels):
    """Raise a ValueError if any of the pulses in records overlap
    Assumes records is already sorted by time.
    """
    last_end = np.zeros(n_channels, dtype=np.int64)
    channel, time = _check_overlaps(records, last_end)
    if channel != -9999:
        raise ValueError(
            f"Bad data! In channel {channel}, a pulse starts at {time}, "
            f"BEFORE the previous pulse in that same channel ended "
            f"(at {last_end[channel]})")


@numba.njit(cache=True, nogil=True)
def _check_overlaps(records, last_end):
    for r in records:
        if r['time'] < last_end[r['channel']]:
            return r['channel'], r['time']
        last_end[r['channel']] = strax.endtime(r)
    return -9999, -9999

@export
@numba.jit(nopython=True, nogil=True, cache=True)
def baseline_per_channel(records, baseline_samples=40, flip=False,pmt_channel=7,
             allow_sloppy_chunking=False, fallback_baseline=16000):
    """Determine baseline as the average of the first baseline_samples
    of each pulse in each channel. Subtract the pulse data from int(baseline),
    and store the baseline mean and rms.

    :param baseline_samples: number of samples at start of pulse to average
    to determine the baseline.
    :param flip: If true, flip sign of data (only for PMT data)
    :param allow_sloppy_chunking: Allow use of the fallback_baseline in case
    the 0th fragment of a pulse is missing
    :param fallback_baseline: Fallback baseline (ADC counts)

    Assumes records are sorted in time (or at least by channel, then time).

    Assumes record_i information is accurate -- so don't cut pulses before
    baselining them!
    """
    # Select the channel and we calculate the baseline per channel
    if not len(records):
        return records

    # Array for looking up last baseline (mean, rms) seen in channel
    # We only care about the channels in this set of records; a single .max()
    # is worth avoiding the hassle of passing n_channels around
    n_channels = records['channel'].max() + 1
    last_bl_in = np.zeros((n_channels, 2), dtype=np.float32)
    seen_first = np.zeros(n_channels, dtype=np.bool_)

    for d_i, d in enumerate(records):

        # Compute the baseline if we're the first record of the pulse,
        # otherwise take the last baseline we've seen in the channel
        if d['record_i'] == 0:
            seen_first[d['channel']] = True
            w = d['data'][:baseline_samples]
            last_bl_in[d['channel']] = bl, rms = w.mean(), w.std()
        else:
            bl, rms = last_bl_in[d['channel']]
            if not seen_first[d['channel']]:
                if not allow_sloppy_chunking:
                    print(d.time, d.channel, d.record_i)
                    raise RuntimeError("Cannot baseline, missing 0th fragment!")
                bl = last_bl_in[d['channel']] = fallback_baseline
                rms = np.nan

        # Subtract baseline from all data samples in the record
        # (any additional zeros should be kept at zero)
        if flip:
            d['data'][:d['length']] = ((-1) * (d['data'][:d['length']] - int(bl)))
        d['baseline'] = bl
        d['baseline_rms'] = rms
