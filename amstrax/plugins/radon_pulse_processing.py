import numba
import numpy as np
import strax
from immutabledict import immutabledict

import amstrax

export, __all__ = strax.exporter()
__all__ += ['NO_PULSE_COUNTS']

# These are also needed in peaklets, since hitfinding is repeated -> then import with the plugin hitself
HITFINDER_OPTIONS = tuple([
    strax.Option(
        'hit_min_amplitude',
        default='pmt_commissioning_initial',
        help='Minimum hit amplitude in ADC counts above baseline. '
             'See straxen.hit_min_amplitude for options.'
    ),
    strax.Option(
        'hit_threshold',
        default=10,
        help='Hitfinder threshold in ADC counts above baseline'
        )
    ])


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
        help='Number of TPC PMTs'),
    strax.Option(
        'check_raw_record_overlaps',
        default=False, track=False, infer_type=False,
        help='Crash if any of the pulses in raw_records overlap with others '
             'in the same channel'),
    strax.Option(
        'allow_sloppy_chunking',
        default=True, track=False, infer_type=False,
        help=('Use a default baseline for incorrectly chunked fragments. '
              'This is a kludge for improperly converted XENON1T data.')),
    *HITFINDER_OPTIONS)
class RadonPulseProcessing(strax.Plugin):
    """
    Get the specif raw_records of the measurements 
    (raw_records_v1724 or raw_records_v1730)
    and split the raw_records into:
    - radon_records
    - radon_pulse_counts
    Apply basic pulse processing:
        1. Flip the pulse, calculate the baseline and integrate the waveform
        2. Find hits, and zero outside hits.

    pulse_counts holds some average information for the individual PMT
    channels for each chunk of raw_records. This includes e.g.
    number of recorded pulses, lone_pulses (pulses which do not
    overlap with any other pulse), or mean values of baseline and
    baseline rms channel.
    """
    __version__ = '0.2.29'
    
    parallel = 'process'
    rechunk_on_save = immutabledict(
        radon_records=False,
        radon_pulse_counts=True)
    compressor = 'zstd'

    depends_on = ('raw_records_v1724','raw_records_v1730')

    provides = ('radon_records', 'radon_pulse_counts')
    data_kind = {k: k for k in provides}
    save_when = strax.SaveWhen.TARGET
       
    def infer_dtype(self,):
        # The record_length is the same for both raw_records_v1724 and raw_records_v1730
        # therefore, we can just use one of the two
        self.record_length = strax.record_length_from_dtype(
            self.deps['raw_records_v1724'].dtype_for('raw_records_v1724'))

        dtype = dict()
        for p in self.provides:
            if 'radon_records' in p:
                dtype[p] = strax.record_dtype(self.record_length)
        dtype['radon_pulse_counts'] = pulse_count_dtype(self.config['n_tpc_pmts'])

        return dtype

    def compute(self, raw_records_v1724, raw_records_v1730, start, end):       
        ####
        # Get the raw_records having data
        ####
        # Case 1: return True -> v1724 measurement
        if self.choose_records(raw_records_v1724,raw_records_v1730):
            raw_records = raw_records_v1724
        # Case 2: return False ->  v1730 measurement
        elif not self.choose_records(raw_records_v1724,raw_records_v1730):
            raw_records = raw_records_v1730
        # For the sake of the if-else statement    
        else:
            pass

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

        strax.baseline(r,
                       baseline_samples=self.config['baseline_samples'],
                       allow_sloppy_chunking=self.config['allow_sloppy_chunking'],
                       flip=True)

        strax.integrate(r)       

        radon_pulse_counts = count_pulses(r, self.config['n_tpc_pmts'])
        radon_pulse_counts['time'] = start
        radon_pulse_counts['endtime'] = end

        if len(r):
            # Find hits
            # -- before filtering,since this messes with the with the S/N
            hits = strax.find_hits(
                r, min_amplitude=amstrax.hit_min_amplitude(
                    self.config['hit_min_amplitude']))

            if self.config['pmt_pulse_filter']:
                # Filter to concentrate the PMT pulses
                strax.filter_records(
                    r, np.array(self.config['pmt_pulse_filter']))

            le, re = self.config['save_outside_hits']
            r = strax.cut_outside_hits(r, hits,
                                       left_extension=le,
                                       right_extension=re)

            # Probably overkill, but just to be sure...
            strax.zero_out_of_bounds(r)

        return dict(radon_records=r,
                    radon_pulse_counts=radon_pulse_counts)

    ###
    # Make one records for future processing
    ###

    def choose_records(self, raw_records_v1724, raw_records_v1730):
        """
        This function implements a decisional loop over which raw_records to keep.
        There are for possibilities:
            1. V1724 measurements 
                    - data only from raw_records_v1724
                    - return True
            2. V1730 measurements
                    - data only from raw_records_v1730
                    - return False
            3. We have data in both raw_records_v1724 and raw_records_v1730
                    - this is not possible in XAMSL data campaign
                    - raise ValueError
            4. No data in both raw_records_v1724 and raw_records_v1730
                    - this is not possible in XAMSL data campaign
                    - raise ValueError          
        For XAMSL measurements details, please have a look:
            https://github.com/XAMS-nikhef/amstrax_files/tree/master/data
        """

        try:
            # Case v1724 measurement: data only in the raw_records_v1724
            if (np.any(raw_records_v1724['channel']) and not np.any(raw_records_v1730['channel'])):
                ret = True
            # Case v1730 measurement: data only in the raw_records_v1730
            elif (np.any(raw_records_v1730['channel']) and not np.any(raw_records_v1724['channel'])):
                ret = False
            # raw_records_v1730 and raw_records_v1724 both returning data
            elif (np.any(raw_records_v1730['channel']) and np.any(raw_records_v1724['channel'])):
                raise ValueError(
                    f"Bad data! The raw_records are both returning data and"
                    f"this is not possible for a XAMSL measurement."
                    f"Check the raw_records data of this measurement"
                    f"or do not process it.")
            # no data in both raw_records_v1730 and raw_records_v1724
            elif (not np.any(raw_records_v1730['channel']) and not np.any(raw_records_v1724['channel'])):
                raise ValueError(
                    f"Bad data! The raw_records are both empty and"
                    f"this is not possible for a XAMSL measurement."
                    f"Check the raw_records data of this measurement"
                    f"or do not process it.")               
            # For the sake of the if-else statement 
            else:
                pass 
        except Exception as e:
            print(f"We've got a {type(e)} error in choose_records() and {str(e)}.")
        return ret

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
