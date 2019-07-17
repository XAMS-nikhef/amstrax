import numba
import numpy as np

import strax
from straxen import get_to_pe
export, __all__ = strax.exporter()

# Number of TPC PMTs. Hardcoded for now...
n_tpc = 248

@export
@strax.takes_config(
    strax.Option('software_zle_channels', default = [], help= 'Channels to apply software ZLE to'),
    strax.Option('software_zle_hitfinder_threshold', default = 15, help= 'Min ADC count threshold used if ZLE is applied'),
    strax.Option('software_zle_extension', default=50, help='Number of samples to save around a hit')
)
class PulseProcessing(strax.Plugin):
    __version__ = '0.0.3'
    depends_on = ('raw_records',)
    provides = 'records'   # TODO: indicate cuts have been done?
    data_kind = 'records'
    compressor = 'zstd'
    parallel = True
    rechunk_on_save = False
    dtype = strax.record_dtype()

    def compute(self, raw_records):
        # Select the records corresponding to channels that need software zle
        # to_zle = np.any(np.array([raw_records['channel'] == channel
                                  # for channel in self.config['software_zle_channels']]), axis=0)

        hits = strax.find_hits(raw_records, threshold = self.config['software_zle_hitfinder_threshold'])
        r = fill_records(raw_records,hits, trigger_window = self.config['software_zle_extension'])

        r = strax.sort_by_time(r)
        # strax.baseline(r)
        strax.integrate(r)
        return r


@numba.njit
def rough_sum(regions, records, to_pe, n, dt):
    """Compute ultra-rough sum waveforms for regions, assuming:
     - every record is a single peak at its first sample
     - all regions have the same length and dt
    and probably not carying too much about boundaries
    """
    if not len(regions) or not len(records):
        return

    # dt and n are passed explicitly to avoid overflows/wraparounds
    # related to the small dt integer type

    peak_i = 0
    r_i = 0
    while (peak_i <= len(regions) - 1) and (r_i <= len(records) - 1):

        p = regions[peak_i]
        l = p['time']
        r = l + n * dt

        while True:
            if r_i > len(records) - 1:
                # Scan ahead until records contribute
                break
            t = records[r_i]['time']
            if t >= r:
                break
            if t >= l:
                index = int((t - l) // dt)
                regions[peak_i]['data'][index] += (
                        records[r_i]['area'] * to_pe[records[r_i]['channel']])
            r_i += 1
        peak_i += 1


##
# Pulse counting
##

@export
def pulse_count_dtype(n_channels):
    # NB: don't use the dt/length interval dtype, integer types are too small
    # to contain these huge chunk-wide intervals
    return [
        (('Lowest start time observed in the chunk', 'time'), np.int64),
        (('Highest endt ime observed in the chunk', 'endtime'), np.int64),
        (('Number of pulses', 'pulse_count'),
         (np.int64, n_channels)),
        (('Number of lone pulses', 'lone_pulse_count'),
         (np.int64, n_channels)),
        (('Integral of all pulses in ADC_count x samples', 'pulse_area'),
         (np.int64, n_channels)),
        (('Integral of lone pulses in ADC_count x samples', 'lone_pulse_area'),
         (np.int64, n_channels)),
    ]


def count_pulses(records, n_channels):
    """Return array with one element, with pulse count info from records"""
    result = np.zeros(1, dtype=pulse_count_dtype(n_channels))
    _count_pulses(records, n_channels, result)
    return result


@numba.njit
def _count_pulses(records, n_channels, result):
    count = np.zeros(n_channels, dtype=np.int64)
    lone_count = np.zeros(n_channels, dtype=np.int64)
    area = np.zeros(n_channels, dtype=np.int64)
    lone_area = np.zeros(n_channels, dtype=np.int64)

    last_end_seen = 0
    next_start = 0
    for r_i, r in enumerate(records):
        if r_i != len(records) - 1:
            next_start = records[r_i + 1]['time']

        ch = r['channel']
        if ch >= n_channels:
            print(ch)
            raise RuntimeError("Out of bounds channel in get_counts!")

        if r['record_i'] == 0:
            count[ch] += 1
            area[ch] += r['area']

            if (r['time'] > last_end_seen
                    and r['time'] + r['pulse_length'] < next_start):
                lone_count[ch] += 1
                lone_area[ch] += r['area']

        last_end_seen = max(last_end_seen,
                            r['time'] + r['pulse_length'])

    res = result[0]
    res['pulse_count'][:] = count[:]
    res['lone_pulse_count'][:] = lone_count[:]
    res['pulse_area'][:] = area[:]
    res['lone_pulse_area'][:] = lone_area[:]
    res['time'] = records[0]['time']
    res['endtime'] = last_end_seen


##
# Misc
##

@numba.njit
def _mask_and_not(x, mask):
    return x[mask], x[~mask]


@export
@numba.njit
def channel_split(rr, first_other_ch):
    """Return """
    return _mask_and_not(rr, rr['channel'] < first_other_ch)

@export
@strax.growing_result(strax.record_dtype(), chunk_size=int(1e4))
@numba.jit(nopython=False, nogil=True, cache=True)
def fill_records(raw_records, hits, trigger_window,_result_buffer = None):

    samples_per_record = strax.DEFAULT_RECORD_LENGTH
    tw = trigger_window
    buffer = _result_buffer

    offset = 0
    skipper = 0

    for i, h in enumerate(hits):
        if skipper != 0:
            # print(skipper)
            skipper -= 1
            continue

        hit = []
        hit.append(h)
        h_c = hits[hits['channel'] == h['channel']]
        for h_ in h_c:
            if h_['time'] > hit[-1]['time'] and h_['time'] < (hit[-1]['time'] + h['dt'] * tw):
                hit.append(h_)
            if h_['time'] > (hit[-1]['time'] + h['dt'] * tw):
                break

        hit_buffer = np.zeros(len(hit), dtype=strax.hit_dtype)
        for i in range(len(hit)):
            hit_buffer[i] = hit[i]

        dt = hit_buffer[0]['dt']
        start = np.min(hit_buffer['time']) - dt * tw
        end = np.max(hit_buffer['time']) + dt * tw
        p_length = int((end - start) / dt)
        records_needed = int(np.ceil((end - start) / (samples_per_record * dt)))

        p_offset = hit_buffer[0]['left'] - tw
        p_end = hit_buffer[-1]['right'] + tw
        input_record_index = [np.unique(hit_buffer['record_i']).tolist()][0]

        if p_offset < 0:
            p_offset += samples_per_record
            input_record_index.append(hit_buffer[0]['record_i'] - 7)


        if p_end > samples_per_record:
            input_record_index.append(hit_buffer[-1]['record_i'] + 7)

        input_record_index.sort()
        record_buffer = []
        for i in input_record_index:
            record_buffer.extend(list(raw_records[i]['data']))

        n_store = 0
        for rec_i in range(records_needed):
            r_ = buffer[offset + rec_i]
            r_['dt'] = dt
            r_['channel'] = hit_buffer[0]['channel']
            r_['pulse_length'] = p_length
            r_['record_i'] = rec_i
            r_['time'] = start + rec_i * samples_per_record * dt
            r_['baseline'] = raw_records[i]['baseline']

            p_offset += n_store
            if rec_i != records_needed - 1:
                n_store = samples_per_record
            else:
                n_store = p_length - samples_per_record * rec_i
            r_['data'][:n_store] = record_buffer[p_offset:p_offset + n_store]
            r_['length'] = n_store

        skipper = len(hit_buffer) - 1
        offset += records_needed
        if offset >= 750:
            yield offset
            offset = 0

    print('Almost done!')
    yield offset
