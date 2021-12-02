import numba
import numpy as np

import strax

export, __all__ = strax.exporter()

# Number of TPC PMTs. Hardcoded for now...
n_tpc = 8


@export
@strax.takes_config(
    strax.Option(
        'save_outside_hits',
        default=(3, 3),
        help='Save (left, right) samples besides hits; cut the rest'),
    strax.Option('trigger_threshold', default=50),
)
class PulseProcessing(strax.Plugin):
    """
    1. Split raw_records into:
     - tpc_records
     - diagnostic_records
     - aqmon_records
    Perhaps this should be done by DAQreader in the future

    For TPC records, apply basic processing:

    2. Apply software HE veto after high-energy peaks.
    3. Find hits, apply linear filter, and zero outside hits.
    """
    __version__ = '0.0.3'

    parallel = 'process'
    rechunk_on_save = False
    compressor = 'zstd'

    depends_on = 'raw_records'

    provides = ('records_alt_bl', 'pulse_counts')
    data_kind = {k: k for k in provides}

    def infer_dtype(self):
        # Get record_length from the plugin making raw_records
        rr_dtype = self.deps['raw_records'].dtype_for('raw_records')
        record_length = len(np.zeros(1, rr_dtype)[0]['data'])

        dtype = dict()
        for p in self.provides:
            if p.endswith('records_alt_bl'):
                dtype[p] = record_dtype(record_length)

        dtype['pulse_counts'] = pulse_count_dtype(n_tpc)
        return dtype

    def compute(self, raw_records):
        # Do not trust in DAQ + strax.baseline to leave the
        # out-of-bounds samples to zero.
        records = np.zeros(len(raw_records), dtype=self.dtype['records_alt_bl'])
        for name in raw_records.dtype.names:
            records[name] = raw_records[name]
        baseline_std(records)
        strax.zero_out_of_bounds(records)

        ##
        # Split off non-TPC records and count TPC pulses
        # (perhaps we should migrate this to DAQRreader in the future)
        ##
        r, other = channel_split(records, n_tpc)
        pulse_counts = count_pulses(r, n_tpc)

        # Find hits
        # -- before filtering,since this messes with the with the S/N
        hits = find_hits(r, threshold=self.config['trigger_threshold'])

        le, re = self.config['save_outside_hits']
        r = strax.cut_outside_hits(r, hits,
                                   left_extension=le,
                                   right_extension=re)

        # Probably overkill, but just to be sure...
        strax.zero_out_of_bounds(r)

        return dict(records_alt_bl=r,
                    pulse_counts=pulse_counts,
                    )


@export
@strax.takes_config(
    strax.Option('peak_gap_threshold', default=3000,
                 help="No hits for this many ns triggers a new peak"),
    strax.Option('peak_left_extension', default=80,
                 help="Include this many ns left of hits in peaks"),
    strax.Option('peak_right_extension', default=80,
                 help="Include this many ns right of hits in peaks"),
    strax.Option('peak_min_area', default=1,
                 help="Minimum contributing PMTs needed to define a peak"),
    strax.Option('peak_min_pmts', default=1,
                 help="Minimum contributing PMTs needed to define a peak"),
    strax.Option('single_channel_peaks', default=False,
                 help='Whether single-channel peaks should be reported'),
    strax.Option('peak_split_min_height', default=25,
                 help="Minimum height in PE above a local sum waveform"
                      "minimum, on either side, to trigger a split"),
    strax.Option('peak_split_min_ratio', default=4,
                 help="Minimum ratio between local sum waveform"
                      "minimum and maxima on either side, to trigger a split"),
    strax.Option('diagnose_sorting', track=False, default=False,
                 help="Enable runtime checks for sorting and disjointness"),
    strax.Option('pmt_channel', default=0,
                 help="PMT channel for splitting pmt and sipms"),
    strax.Option('trigger_threshold', default=50),
)
class PeaksAltBl(strax.Plugin):
    depends_on = 'records_alt_bl'
    data_kind = dict(peaks_top_alt_bl='peaks',
                     peaks_bottom_alt_bl='peaks')
    parallel = 'process'
    provides = ('peaks_top_alt_bl', 'peaks_bottom_alt_bl')
    rechunk_on_save = True

    __version__ = '0.1.12'
    dtype = dict(peaks_top_alt_bl=strax.peak_dtype(n_channels=8)
                                  + [(('Maximum height of the peak', 'peak_max'), np.int16)],
                 peaks_bottom_alt_bl=strax.peak_dtype(n_channels=8)
                                     + [(('Maximum height of the peak', 'peak_max'), np.int16)]
                 )

    def compute(self, records_alt_bl):
        r = records_alt_bl
        self.to_pe = np.ones(16)

        hits = find_hits(r, threshold=self.config['trigger_threshold'])

        hits = strax.sort_by_time(hits)
        hits_bottom, hits_top = hits[hits['channel'] == self.config['pmt_channel']], hits[
            hits['channel'] != self.config['pmt_channel']]
        r_bottom, r_top = r[r['channel'] == self.config['pmt_channel']], r[
            r['channel'] != self.config['pmt_channel']]

        peaks_bottom = strax.find_peaks(
            hits_bottom, self.to_pe,
            gap_threshold=self.config['peak_gap_threshold'],
            left_extension=self.config['peak_left_extension'],
            right_extension=self.config['peak_right_extension'],
            min_channels=1,
            result_dtype=self.dtype['peaks_bottom_alt_bl'])
        strax.sum_waveform(peaks_bottom, r_bottom, self.to_pe)

        # peaks_bottom = strax.split_peaks(
        #     peaks_bottom, r_bottom, self.to_pe,
        #     min_height=self.config['peak_split_min_height'],
        #     min_ratio=self.config['peak_split_min_ratio'])

        strax.compute_widths(peaks_bottom)

        peaks_top = strax.find_peaks(
            hits_top, self.to_pe,
            gap_threshold=self.config['peak_gap_threshold'],
            left_extension=self.config['peak_left_extension'],
            right_extension=self.config['peak_right_extension'],
            min_area=self.config['peak_min_area'],
            min_channels=self.config['peak_min_pmts'],
            result_dtype=self.dtype['peaks_top_alt_bl'])
        strax.sum_waveform(peaks_top, r_top, self.to_pe)

        # peaks_top = strax.split_peaks(
        #     peaks_top, r_top, self.to_pe,
        #     min_height=self.config['peak_split_min_height'],
        #     min_ratio=self.config['peak_split_min_ratio'])

        strax.compute_widths(peaks_top)

        peaks_top['peak_max'] = np.max(peaks_top['data'], axis=1)
        peaks_bottom['peak_max'] = np.max(peaks_bottom['data'], axis=1)

        return dict(peaks_top_alt_bl=peaks_top,
                    peaks_bottom_alt_bl=peaks_bottom,
                    )


@export
class PeakBasicsTopAltBl(strax.Plugin):
    provides = 'peak_basics_top_alt_bl'
    depends_on = 'peaks_top_alt_bl'
    data_kind = 'peaks'
    parallel = 'False'
    rechunk_on_save = True
    __version__ = '0.1.0'
    dtype = [
        (('Start time of the peak (ns since unix epoch)',
          'time'), np.int64),
        (('End time of the peak (ns since unix epoch)',
          'endtime'), np.int64),
        (('Peak integral in PE',
          'area'), np.float32),
        (('Number of PMTs contributing to the peak',
          'n_channels'), np.int16),
        (('PMT number which contributes the most PE',
          'max_pmt'), np.int16),
        (('Area of signal in the largest-contributing PMT (PE)',
          'max_pmt_area'), np.int32),
        (('Width (in ns) of the central 50% area of the peak',
          'range_50p_area'), np.float32),
        (('Fraction of area seen by the top array',
          'area_fraction_top'), np.float32),

        (('Length of the peak waveform in samples',
          'length'), np.int32),
        (('Time resolution of the peak waveform in ns',
          'dt'), np.int16),
    ]

    def compute(self, peaks):
        p = peaks
        p = strax.sort_by_time(p)
        r = np.zeros(len(p), self.dtype)
        for q in 'time length dt area'.split():
            r[q] = p[q]
        r['endtime'] = p['time'] + p['dt'] * p['length']
        r['n_channels'] = (p['area_per_channel'] > 0).sum(axis=1)
        r['range_50p_area'] = p['width'][:, 5]
        r['max_pmt'] = np.argmax(p['area_per_channel'], axis=1)
        r['max_pmt_area'] = np.max(p['area_per_channel'], axis=1)

        area_top = p['area_per_channel'][:, :8].sum(axis=1)
        # Negative-area peaks get 0 AFT - TODO why not NaN?
        m = p['area'] > 0
        r['area_fraction_top'][m] = area_top[m] / p['area'][m]
        return r


@export
class PeakBasicsBottomAltBl(strax.Plugin):
    provides = 'peak_basics_bottom_alt_bl',
    depends_on = 'peaks_bottom_alt_bl',
    data_kind = 'peaks',
    parallel = 'False',
    rechunk_on_save = True,
    __version__ = '0.1.0',
    dtype = [
        (('Start time of the peak (ns since unix epoch)',
          'time'), np.int64),
        (('End time of the peak (ns since unix epoch)',
          'endtime'), np.int64),
        (('Peak integral in PE',
          'area'), np.float32),
        (('Number of PMTs contributing to the peak',
          'n_channels'), np.int16),
        (('PMT number which contributes the most PE',
          'max_pmt'), np.int16),
        (('Area of signal in the largest-contributing PMT (PE)',
          'max_pmt_area'), np.int32),
        (('Width (in ns) of the central 50% area of the peak',
          'range_50p_area'), np.float32),
        (('Fraction of area seen by the top array',
          'area_fraction_top'), np.float32),

        (('Length of the peak waveform in samples',
          'length'), np.int32),
        (('Time resolution of the peak waveform in ns',
          'dt'), np.int16),
    ]

    def compute(self, peaks):
        p = peaks
        p = strax.sort_by_time(p)
        r = np.zeros(len(p), self.dtype)
        for q in 'time length dt area'.split():
            r[q] = p[q]
        r['endtime'] = p['time'] + p['dt'] * p['length']
        r['n_channels'] = (p['area_per_channel'] > 0).sum(axis=1)
        r['range_50p_area'] = p['width'][:, 5]
        r['max_pmt'] = np.argmax(p['area_per_channel'], axis=1)
        r['max_pmt_area'] = np.max(p['area_per_channel'], axis=1)

        area_top = p['area_per_channel'][:, :8].sum(axis=1)
        # Negative-area peaks get 0 AFT - TODO why not NaN?
        m = p['area'] > 0
        r['area_fraction_top'][m] = area_top[m] / p['area'][m]
        return r


# Base dtype for interval-like objects (pulse, peak, hit)
interval_dtype = [
    (('Channel/PMT number',
      'channel'), np.int16),
    (('Time resolution in ns',
      'dt'), np.int16),
    (('Start time of the interval (ns since unix epoch)',
      'time'), np.int64),
    # Don't try to make O(second) long intervals!
    (('Length of the interval in samples',
      'length'), np.int32),
    # Sub-dtypes MUST contain an area field
    # However, the type varies: float for sum waveforms (area in PE)
    # and int32 for per-channel waveforms (area in ADC x samples)
]


def record_dtype(samples_per_record=strax.DEFAULT_RECORD_LENGTH):
    """Data type for a waveform record.

    Length can be shorter than the number of samples in data,
    this indicates a record with zero-padding at the end.
    """
    return interval_dtype + [
        (("Integral in ADC x samples",
          'area'), np.int32),
        # np.int16 is not enough for some PMT flashes...
        (('Length of pulse to which the record belongs (without zero-padding)',
          'pulse_length'), np.int32),
        (('Fragment number in the pulse',
          'record_i'), np.int16),
        (('Baseline in ADC counts. data = int(baseline) - data_orig',
          'baseline'), np.float32),
        (('Level of data reduction applied (strax.ReductionLevel enum)',
          'reduction_level'), np.uint8),
        # Note this is defined as a SIGNED integer, so we can
        # still represent negative values after subtracting baselines
        (('Waveform data in ADC counts above baseline',
          'data'), np.int16, samples_per_record),
        (('Baseline standard deviation',
          'baseline_std'), np.float32),
    ]


@numba.jit(nopython=True, nogil=True, cache=True)
def baseline_std(records, baseline_samples=40):
    """Subtract pulses from int(baseline), store baseline in baseline field
    :param baseline_samples: number of samples at start of pulse to average
    Assumes records are sorted in time (or at least by channel, then time)

    Assumes record_i information is accurate (so don't cut pulses before
    baselining them!)
    """
    if not len(records):
        return records

    # Array for looking up last baseline seen in channel
    # We only care about the channels in this set of records; a single .max()
    # is worth avoiding the hassle of passing n_channels around
    last_bl_std_in = np.zeros(records['channel'].max() + 1, dtype=np.float32)

    for d_i, d in enumerate(records):

        # Compute the baseline if we're the first record of the pulse,
        # otherwise take the last baseline we've seen in the channel
        if d.record_i == 0:
            bl_std = last_bl_std_in[d.channel] = d.data[:baseline_samples].std()
        else:
            bl_std = last_bl_std_in[d.channel]

        d.baseline_std = bl_std


@strax.growing_result(strax.hit_dtype, chunk_size=int(1e4))
@numba.jit(nopython=True, nogil=True, cache=True)
def find_hits(records, threshold=70, _result_buffer=None):
    """Return hits (intervals above threshold) found in records.
    Hits that straddle record boundaries are split (TODO: fix this?)

    NB: returned hits are NOT sorted yet!
    """
    buffer = _result_buffer
    if not len(records):
        return
    samples_per_record = len(records[0]['data'])
    offset = 0

    for record_i, r in enumerate(records):
        # print("Starting record ', record_i)
        in_interval = False
        hit_start = -1
        area = 0

        for i in range(samples_per_record):
            # We can't use enumerate over r['data'],
            # numba gives errors if we do.
            # TODO: file issue?
            x = r['data'][i]
            above_threshold = x > threshold
            # print(r['data'][i], above_threshold, in_interval, hit_start)

            if not in_interval and above_threshold:
                # Start of a hit
                in_interval = True
                hit_start = i

            if in_interval:
                if not above_threshold:
                    # Hit ends at the start of this sample
                    hit_end = i
                    in_interval = False

                elif i == samples_per_record - 1:
                    # Hit ends at the *end* of this sample
                    # (because the record ends)
                    hit_end = i + 1
                    area += x
                    in_interval = False

                else:
                    area += x

                if not in_interval:
                    # print('saving hit')
                    # Hit is done, add it to the result
                    if hit_end == hit_start:
                        print(r['time'], r['channel'], hit_start)
                        raise ValueError(
                            "Caught attempt to save zero-length hit!")
                    res = buffer[offset]
                    res['left'] = hit_start
                    res['right'] = hit_end
                    res['time'] = r['time'] + hit_start * r['dt']
                    # Note right bound is exclusive, no + 1 here:
                    res['length'] = hit_end - hit_start
                    res['dt'] = r['dt']
                    res['channel'] = r['channel']
                    res['record_i'] = record_i
                    area += int(round(
                        res['length'] * (r['baseline'] % 1)))
                    res['area'] = area
                    area = 0

                    # Yield buffer to caller if needed
                    offset += 1
                    if offset == len(buffer):
                        yield offset
                        offset = 0

                    # Clear stuff, just for easier debugging
                    # hit_start = 0
                    # hit_end = 0
    yield offset


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


@numba.jit(nopython=True, nogil=True, cache=True)
def get_record_index(raw_records, channel, direction):
    if direction == -1:
        for i in range(-1, -len(raw_records), -1):
            if raw_records[i]['channel'] == channel:
                return i
        else:
            return 0

    if direction == +1:
        for i in range(1, len(raw_records), 1):
            if raw_records[i]['channel'] == channel:
                return i
        else:
            return len(raw_records)


@export
@strax.growing_result(strax.record_dtype(), chunk_size=int(1e6))
@numba.jit(nopython=False, nogil=False, cache=True)
def fill_records(raw_records, hits, trigger_window, _result_buffer=None):
    samples_per_record = strax.DEFAULT_RECORD_LENGTH
    tw = trigger_window
    buffer = _result_buffer

    offset = 0
    skipper = 0
    for ch in np.unique(hits['channel']):
        hit_ch = hits[(hits['channel'] == ch)
                      & (hits['length'] > 1)]
        for h in hit_ch:
            if skipper != 0:
                skipper -= 1
                continue
            dt = h['dt']
            hit = []
            hit.append(h)
            h_c = hit_ch[(hit_ch['time'] > h['time'])]
            max_t = h['time']
            for h_ in h_c:
                if h_['time'] > max_t and h_['time'] < max_t + dt * tw:
                    hit.append(h_)
                    max_t = h_['time']
                elif h_['time'] > max_t + dt * tw:
                    break

            hit_buffer = np.zeros(len(hit), dtype=strax.hit_dtype)
            for i in np.arange(len(hit)):
                # print(hit[i])
                hit_buffer[i] = hit[i]

            dt = hit_buffer[0]['dt']
            start = hit_buffer[0]['time'] - dt * tw
            end = hit_buffer[-1]['time'] + dt * tw
            p_length = int((end - start) / dt)
            # records_needed = int(np.ceil(p_length / (samples_per_record)))

            p_offset = hit[0]['left'] - tw
            p_end = hit[-1]['right'] + tw

            input_record_index = [np.unique(hit_buffer['record_i']).tolist()][0]

            assert input_record_index != [0]
            #     print('Dit hoort niet')
            #     print(hit)
            #     continue
            if p_offset < 0:
                previous = hit_buffer[0]['record_i'] + get_record_index(
                    raw_records[:hit_buffer[0]['record_i']],
                    hit_buffer[0]['channel'],
                    -1)
                # print(previous)
                if previous < 0 or previous == hit_buffer[0]['record_i']:
                    p_length += p_offset
                    p_offset = 0

                if previous > 0:
                    p_offset += samples_per_record
                    input_record_index.append(previous)

            if p_end > samples_per_record:
                next = hit_buffer[-1]['record_i'] + get_record_index(
                    raw_records[hit_buffer[-1]['record_i']:],
                    hit_buffer[-1]['channel'],
                    +1)
                # print(next)
                if next < len(raw_records):
                    input_record_index.append(next)

                if next > len(raw_records):
                    p_length -= (p_end % samples_per_record)
                    # print('hmm')

            # if len(np.unique(raw_records['channel'][input_record_index])) !=1:
            #     print('fuck')

            input_record_index.sort()
            record_buffer = []
            # print(input_record_index)
            for i in input_record_index:
                if i > len(raw_records) - 1:
                    p_length -= tw * dt
                    # print('here')
                    continue
                record_buffer.extend(list(raw_records[i]['data']))

            records_needed = int(np.ceil(p_length / samples_per_record))

            n_store = 0
            for rec_i in range(records_needed):
                r_ = buffer[offset + rec_i]
                r_['dt'] = dt
                r_['channel'] = hit[0]['channel']
                r_['pulse_length'] = p_length
                r_['record_i'] = rec_i
                r_['time'] = start + rec_i * samples_per_record * dt
                r_['baseline'] = raw_records[input_record_index[0]]['baseline']

                p_offset += n_store
                if rec_i != records_needed - 1:
                    n_store = samples_per_record
                else:
                    n_store = p_length - samples_per_record * rec_i
                # print(p_length , records_needed, rec_i, n_store, p_offset , len(record_buffer))
                r_['data'][:n_store] = record_buffer[p_offset:p_offset + n_store]
                r_['length'] = n_store

            skipper = len(hit) - 1
            offset += records_needed
            if offset >= 750:
                yield offset
                offset = 0

    print('Almost done!')
    yield offset
