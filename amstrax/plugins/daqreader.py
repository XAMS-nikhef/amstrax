import glob
import warnings
from immutabledict import immutabledict
import numpy as np
import strax
import straxen
from straxen.plugins.daqreader import split_channel_ranges

export, __all__ = strax.exporter()
__all__ += ['ARTIFICIAL_DEADTIME_CHANNEL']


ARTIFICIAL_DEADTIME_CHANNEL = 40
class ArtificialDeadtimeInserted(UserWarning):
    pass


@export
@strax.takes_config(
    # All these must have track=False, so the raw_records hash never changes!
    # DAQ settings -- should match settings given to redax
    strax.Option('record_length', default=110, track=False, type=int,
                 help="Number of samples per raw_record"),
    strax.Option('max_digitizer_sampling_time',
                 default=10, track=False, type=int,
                 help="Highest interval time of the digitizer sampling times(s) used."),
    strax.Option('run_start_time', type=float, track=False, default=0,
                 help="time of start run (s since unix epoch)"),
    strax.Option('daq_chunk_duration', track=False,
                 default=int(5e9), type=int,
                 help="Duration of regular chunks in ns"),
    strax.Option('daq_overlap_chunk_duration', track=False,
                 default=int(5e8), type=int,
                 help="Duration of intermediate/overlap chunks in ns"),
    strax.Option('daq_compressor', default="lz4", track=False,
                 help="Algorithm used for (de)compressing the live data"),
    strax.Option('readout_threads', type=dict, track=False,
                 help="Dictionary of the readout threads where the keys "
                      "specify the reader and value the number of threads"),
    strax.Option('daq_input_dir', type=str, track=False,
                 help="Directory where readers put data"),

    # DAQReader settings
    strax.Option('safe_break_in_pulses', default=1000, track=False,
                 help="Time (ns) between pulses indicating a safe break "
                      "in the datastream -- gaps of this size cannot be "
                      "interior to peaklets."),
    strax.Option('channel_map', track=False, type=immutabledict,
                 help="immutabledict mapping subdetector to (min, max) "
                      "channel number."))
class DAQReader(straxen.DAQReader):
    """
    Read the XAMS DAQ-live_data from redax and split it to the
    appropriate raw_record data-types based on the channel-map.
    Does nothing whatsoever to the live_data; not even baselining.
    Provides: 
        - raw_records_v1724, sampled from the V1724 digitizer with sampling resolution = 10ns
        - raw_records_v1730, sampled from the V1730 digitizer with sampling resolution = 2ns
        - raw_records_aqmon, actually empty unless we need some strax-deadtime
    """
    provides = (
        'raw_records_v1724',
        'raw_records_v1730',
        'raw_records_aqmon',
    )

    depends_on = tuple()
    data_kind = immutabledict(zip(provides, provides))
    parallel = 'process'
    rechunk_on_save = False
    # never change the version!
    __version__ = '0.0.0'
    compressor = 'zstd'

    def infer_dtype(self):
        if not self.multi_output:
            return strax.raw_record_dtype(
                samples_per_record=self.config["record_length"])

        return {
            d: strax.raw_record_dtype(
                samples_per_record=self.config["record_length"])
            for d in self.provides}

    def _load_chunk(self, path, start, end, kind='central'):
        first_provides = self.provides[0]
        records = [
            strax.load_file(
                fn,
                compressor=self.config["daq_compressor"],
                dtype=self.dtype_for(first_provides))
            for fn in sorted(glob.glob(f'{path}/*'))]
        records = np.concatenate(records)
        records = strax.sort_by_time(records)

        first_start, last_start, last_end = None, None, None
        if len(records):
            first_start, last_start = records[0]['time'], records[-1]['time']
            # Records are sorted by (start)time and are of variable length.
            # Their end-times can differ. In the most pessimistic case we have
            # to look back one record length for each channel.
            tot_channels = np.sum([np.diff(x)+1 for x in
                                   self.config['channel_map'].values()])
            look_n_samples = self.config["record_length"] * tot_channels
            last_end = strax.endtime(records[-look_n_samples:]).max()
            if first_start < start or last_start >= end:
                raise ValueError(
                    f"Bad data from DAQ: chunk {path} should contain data "
                    f"that starts in [{start}, {end}), but we see start times "
                    f"ranging from {first_start} to {last_start}.")

        if kind == 'central':
            result = records
            break_time = None
        else:
            # Find a time at which we can safely partition the data.
            min_gap = self.config['safe_break_in_pulses']
            if not len(records) or last_end + min_gap < end:
                # There is enough room at the end of the data
                break_time = end - min_gap
                result = records if kind == 'post' else records[:0]
            else:
                # Let's hope there is some quiet time in the middle
                try:
                    result, break_time = strax.from_break(
                        records,
                        safe_break=min_gap,
                        # Records from the last chunk can extend as far as:
                        not_before=(start
                                    + self.config['record_length'] * self.dt_max),
                        left=kind == 'post',
                        tolerant=False)
                except strax.NoBreakFound:
                    # We still have to break somewhere, but this can involve
                    # throwing away data.
                    # Let's do it at the end of the chunk
                    # TODO: find a better time, e.g. a longish-but-not-quite
                    # satisfactory gap
                    break_time = end - min_gap

                    # Mark the region where data /might/ be removed with
                    # artificial deadtime.
                    dead_time_start = (
                            break_time - self.config['record_length'] * self.dt_max)
                    warnings.warn(
                        f"Data in {path} is so dense that no {min_gap} "
                        f"ns break exists: data loss inevitable. "
                        f"Inserting artificial deadtime between "
                        f"{dead_time_start} and {end}.",
                        ArtificialDeadtimeInserted)

                    if kind == 'pre':
                        # Give the artificial deadtime past the break
                        result = self._artificial_dead_time(
                            start=break_time, end=end, dt=self.dt_max)
                    else:
                        # Remove data that would stick out
                        result = records[strax.endtime(records) <= break_time]
                        # Add the artificial deadtime until the break
                        result = strax.sort_by_time(
                            np.concatenate([result,
                                            self._artificial_dead_time(
                                                start=dead_time_start,
                                                end=break_time, dt=self.dt_max)]))
        return result, break_time

    def _artificial_dead_time(self, start, end, dt):
        return strax.dict_to_rec(
            dict(time=[start],
                 length=[(end - start) // dt],
                 dt=[dt],
                 channel=[ARTIFICIAL_DEADTIME_CHANNEL]),
            self.dtype_for('raw_records'))

    def compute(self, chunk_i):
        dt_central = self.config['daq_chunk_duration']
        dt_overlap = self.config['daq_overlap_chunk_duration']

        t_start = chunk_i * (dt_central + dt_overlap)
        t_end = t_start + dt_central

        pre, current, post = self._chunk_paths(chunk_i)
        r_pre, r_post = None, None
        break_pre, break_post = t_start, t_end

        if pre:
            if chunk_i == 0:
                warnings.warn(
                    f"DAQ is being sloppy: there should be no pre dir {pre} "
                    f"for chunk 0. We're ignoring it.",
                    UserWarning)
            else:
                r_pre, break_pre = self._load_chunk(
                    path=pre,
                    start=t_start - dt_overlap,
                    end=t_start,
                    kind='pre')

        r_main, _ = self._load_chunk(
            path=current,
            start=t_start,
            end=t_end,
            kind='central')

        if post:
            r_post, break_post = self._load_chunk(
                path=post,
                start=t_end,
                end=t_end + dt_overlap,
                kind='post')

        # Concatenate the result.
        records = np.concatenate([
            x for x in (r_pre, r_main, r_post)
            if x is not None])

        # Split records by channel
        result_arrays = split_channel_ranges(
            records,
            np.asarray(list(self.config['channel_map'].values())))
        del records

        # Convert to strax chunks
        result = dict()
        for i, subd in enumerate(self.config['channel_map']):
            if len(result_arrays[i]):
                # dt may differ per subdetector
                dt = result_arrays[i]['dt'][0]
                # Convert time to time in ns since unix epoch.
                # Ensure the offset is a whole digitizer sample
                result_arrays[i]["time"] += dt * (self.t0 // dt)

            # Ignore data from the 'blank' channels, corresponding to
            # channels that have nothing connected
            if subd.endswith('blank'):
                continue

            result_name = 'raw_records'
            result_name += '_' + subd
            result[result_name] = self.chunk(
                start=self.t0 + break_pre,
                end=self.t0 + break_post,
                data=result_arrays[i],
                data_type=result_name)

        print(f"Read chunk {chunk_i:06d} from DAQ")
        for r in result.values():
            # Print data rate / data type if any
            if r._mbs() > 0:
                print(f"\t{r}")
        return result


@export
class Fake1TDAQReader(DAQReader):
    provides = (
        'raw_records',
        'raw_records_diagnostic',
        'raw_records_aqmon')

    data_kind = immutabledict(zip(provides, provides))
