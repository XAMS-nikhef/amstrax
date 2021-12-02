"""Convert pax .zip files to flat records format
"""
import numpy as np
import os
import glob
import numba

import strax

export, __all__ = strax.exporter()


def records_needed(pulse_length, samples_per_record):
    """Return records needed to store pulse_length samples"""
    return np.ceil(pulse_length / samples_per_record).astype(np.int)


@export
def pax_to_records(input_filename,
                   samples_per_record=strax.DEFAULT_RECORD_LENGTH,
                   events_per_chunk=10,
                   dt=10):
    """Return pulse records array from pax zip input_filename
    This only works if you have pax installed in your strax environment,
    which is somewhat tricky.
    """

    # Monkeypatch matplotlib so pax is importable
    # See https://github.com/XENON1T/pax/pull/734
    import matplotlib
    matplotlib._cntr = None

    from pax import core  # Pax is not a dependency

    mypax = core.Processor('XENON1T', config_dict=dict(
        pax=dict(
            look_for_config_in_runs_db=False,
            plugin_group_names=['input'],
            encoder_plugin=None,
            input_name=input_filename),
        # Fast startup: skip loading big maps
        WaveformSimulator=dict(
            s1_light_yield_map='placeholder_map.json',
            s2_light_yield_map='placeholder_map.json',
            s1_patterns_file=None,
            s2_patterns_file=None)))

    print(f"Starting conversion, {events_per_chunk} evt/chunk")

    results = []

    def finish_results():
        nonlocal results
        records = np.concatenate(results)
        # In strax data, records are always stored
        # sorted, baselined and integrated
        records = strax.sort_by_time(records)
        print("Returning %d records" % len(records))
        results = []
        return records

    for event in mypax.get_events():
        event = mypax.process_event(event)

        if not len(event.pulses):
            # Triggerless pax data contains many empty events
            # at the end. With the fixed events per chunk setting
            # this can lead to empty files, which confuses strax.
            continue

        pulse_lengths = np.array([p.length
                                  for p in event.pulses])

        n_records_tot = records_needed(pulse_lengths,
                                       samples_per_record).sum()
        records = np.zeros(n_records_tot,
                           dtype=strax.raw_record_dtype(samples_per_record))
        output_record_index = 0  # Record offset in data

        for p in event.pulses:
            n_records = records_needed(p.length, samples_per_record)

            for rec_i in range(n_records):
                r = records[output_record_index]
                r['dt'] = dt
                r['time'] = (event.start_time
                             + p.left * dt
                             + rec_i * samples_per_record * dt)
                r['channel'] = p.channel
                r['pulse_length'] = p.length
                r['record_i'] = rec_i

                # How much are we storing in this record?
                if rec_i != n_records - 1:
                    # There's more chunks coming, so we store a full chunk
                    n_store = samples_per_record
                    assert p.length > samples_per_record * (rec_i + 1)
                else:
                    # Just enough to store the rest of the data
                    # Note it's not p.length % samples_per_record!!!
                    # (that would be zero if we have to store a full record)
                    n_store = p.length - samples_per_record * rec_i

                assert 0 <= n_store <= samples_per_record
                r['length'] = n_store

                offset = rec_i * samples_per_record
                r['data'][:n_store] = p.raw_data[offset:offset + n_store]
                output_record_index += 1

        results.append(records)
        if len(results) >= events_per_chunk:
            yield finish_results()

    mypax.shutdown()

    if len(results):
        y = finish_results()
        if len(y):
            yield y


def pax_to_records_zle(input_filename,
                       samples_per_record=strax.DEFAULT_RECORD_LENGTH,
                       events_per_chunk=10,
                       dt=10):
    """Return pulse records array from pax zip input_filename
    This only works if you have pax installed in your strax environment,
    which is somewhat tricky.
    """

    # Monkeypatch matplotlib so pax is importable
    # See https://github.com/XENON1T/pax/pull/734
    import matplotlib
    matplotlib._cntr = None

    from pax import core  # Pax is not a dependency

    mypax = core.Processor('XENON1T', config_dict=dict(
        pax=dict(
            look_for_config_in_runs_db=False,
            plugin_group_names=['input'],
            encoder_plugin=None,
            input_name=input_filename),
        # Fast startup: skip loading big maps
        WaveformSimulator=dict(
            s1_light_yield_map='placeholder_map.json',
            s2_light_yield_map='placeholder_map.json',
            s1_patterns_file=None,
            s2_patterns_file=None)))

    print(f"Starting conversion, {events_per_chunk} evt/chunk")

    results = []

    def finish_results():
        nonlocal results
        records = np.concatenate(results)
        # In strax data, records are always stored
        # sorted, baselined and integrated
        records = strax.sort_by_time(records)
        print("Returning %d records" % len(records))
        results = []
        return records

    for event in mypax.get_events():
        event = mypax.process_event(event)

        if not len(event.pulses):
            # Triggerless pax data contains many empty events
            # at the end. With the fixed events per chunk setting
            # this can lead to empty files, which confuses strax.
            continue

        pulse_lengths = np.array([p.length
                                  for p in event.pulses])

        output_record_index = 0  # Record offset in data

        for p in event.pulses:

            for left, right, data in find_intervals(p):
                pulse_length = right - left + 1
                records_needed = int(np.ceil(pulse_length / samples_per_record))

                records = np.zeros(records_needed,
                                   dtype=strax.raw_record_dtype(samples_per_record))
                records['channel'] = p.channel
                records['dt'] = dt
                records['time'] = event.start_time + p.left * dt + dt * (
                        left + samples_per_record * np.arange(records_needed))
                records['length'] = [min(pulse_length, samples_per_record * (i + 1))
                                     - samples_per_record * i for i in range(records_needed)]
                records['pulse_length'] = pulse_length
                records['record_i'] = np.arange(records_needed)
                records['data'] = np.pad(data,
                                         (0, records_needed * samples_per_record - pulse_length),
                                         'constant').reshape((-1, samples_per_record))

                output_record_index += records_needed

                results.append(records)
        if len(results) >= events_per_chunk:
            yield finish_results()

    mypax.shutdown()

    if len(results):
        y = finish_results()
        if len(y):
            yield y


@numba.jit(numba.int32(numba.int16[:], numba.int64, numba.int64, numba.int64[:, :]),
           nopython=True)
def find_intervals_below_threshold(w, threshold, holdoff, result_buffer):
    """Fills result_buffer with l, r bounds of intervals in w < threshold.
    :param w: Waveform to do hitfinding in
    :param threshold: Threshold for including an interval
    :param result_buffer: numpy N*2 array of ints, will be filled by function.
                          if more than N intervals are found, none past the first N will be processed.
    :returns : number of intervals processed
    Boundary indices are inclusive, i.e. the right boundary is the last index which was < threshold
    """
    result_buffer_size = len(result_buffer)
    last_index_in_w = len(w) - 1

    in_interval = False
    current_interval = 0
    current_interval_start = -1
    current_interval_end = -1

    for i, x in enumerate(w):

        if x < threshold:
            if not in_interval:
                # Start of an interval
                in_interval = True
                current_interval_start = i

            current_interval_end = i

        if ((i == last_index_in_w and in_interval) or
                (x >= threshold and i >= current_interval_end + holdoff and in_interval)):
            # End of the current interval
            in_interval = False

            # Add bounds to result buffer
            result_buffer[current_interval, 0] = current_interval_start
            result_buffer[current_interval, 1] = current_interval_end
            current_interval += 1

            if current_interval == result_buffer_size:
                result_buffer[current_interval, 1] = len(w) - 1

    n_intervals = current_interval  # No +1, as current_interval was incremented also when the last interval closed
    return n_intervals


def find_intervals(pulse):
    data = pulse.raw_data
    zle_intervals_buffer = -1 * np.ones((50000, 2), dtype=np.int64)
    # For simulated data taking reference baseline as baseline
    # Operating directly on digitized downward waveform
    threshold = int(np.median(data) - 2 * np.std(data))

    n_itvs_found = find_intervals_below_threshold(
        data,
        threshold=threshold,
        holdoff=101,
        result_buffer=zle_intervals_buffer, )

    itvs_to_encode = zle_intervals_buffer[:n_itvs_found]
    itvs_to_encode[:, 0] -= 50
    itvs_to_encode[:, 1] += 50
    itvs_to_encode = np.clip(itvs_to_encode, 0, len(data) - 1)
    # Land trigger window on even numbers
    itvs_to_encode[:, 0] = np.ceil(itvs_to_encode[:, 0] / 2.0) * 2
    itvs_to_encode[:, 1] = np.floor(itvs_to_encode[:, 1] / 2.0) * 2

    for itv in itvs_to_encode:
        print(itv)
        yield itv[0], itv[1], data[itv[0]:itv[1] + 1]


@export
@strax.takes_config(
    strax.Option('pax_raw_dir', default='/data/xenon/raw', track=False,
                 help="Directory with raw pax datasets"),
    strax.Option('stop_after_zips', default=0, track=False,
                 help="Convert only this many zip files. 0 = all."),
    strax.Option('events_per_chunk', default=50, track=False,
                 help="Number of events to yield per chunk"),
    strax.Option('samples_per_record', default=strax.DEFAULT_RECORD_LENGTH, track=False,
                 help="Number of samples per record")
)
class RecordsFromPax(strax.Plugin):
    provides = 'raw_records'
    data_kind = 'raw_records'
    compressor = 'zstd'
    depends_on = tuple()
    parallel = False
    rechunk_on_save = False
    __version__ = '0.0.2'

    def infer_dtype(self):
        return strax.raw_record_dtype(self.config['samples_per_record'])

    def iter(self, *args, **kwargs):

        if not os.path.exists(self.config['pax_raw_dir']):
            raise FileNotFoundError(self.config['pax_raw_dir'])
        input_dir = os.path.join(self.config['pax_raw_dir'], self.run_id)
        pax_files = sorted(glob.glob(input_dir + '/XAMS*.zip'))
        pax_sizes = np.array([os.path.getsize(x)
                              for x in pax_files])
        print(f"Found {len(pax_files)} files, {pax_sizes.sum() / 1e9:.2f} GB")
        last_endtime = 0
        if self.run_id[-4:] == '1730':
            self.config['dt'] = 2
        else:
            self.config['dt'] = 10

        for file_i, in_fn in enumerate(pax_files):
            if (self.config['stop_after_zips']
                    and file_i >= self.config['stop_after_zips']):
                break
            for records in pax_to_records_zle(
                    in_fn,
                    samples_per_record=self.config['samples_per_record'],
                    events_per_chunk=self.config['events_per_chunk'],
                    dt=self.config['dt']):

                if not len(records):
                    continue
                if last_endtime == 0:
                    last_endtime = records[0]['time']
                new_endtime = strax.endtime(records).max()

                yield self.chunk(start=last_endtime,
                                 end=new_endtime,
                                 data=records)

                last_endtime = new_endtime
