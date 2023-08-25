import numba
import numpy as np
import strax
export, __all__ = strax.exporter()

@export
@strax.takes_config(
    strax.Option('trigger_min_area', default=100,
                 help='Peaks must have more area (PE) than this to '
                      'cause events'),
    strax.Option('trigger_min_competing', default=3,
                 help='Peaks must have More nearby larger or slightly smaller'
                      ' peaks to cause events'),
    strax.Option('left_event_extension', default=int(1e6),
                 help='Extend events this many ns to the left from each '
                      'triggering peak'),
    strax.Option('right_event_extension', default=int(1e6),
                 help='Extend events this many ns to the right from each '
                      'triggering peak'),
)
class Events(strax.OverlapWindowPlugin):
    depends_on = ['peaks', 'peak_basics']  # peak_basics instead of n_competing
    rechunk_on_save = False
    data_kind = 'events'
    parallel = False
    dtype = [
        ('event_number', np.int64, 'Event number in this dataset'),
        ('time', np.int64, 'Event start time in ns since the unix epoch'),
        ('endtime', np.int64, 'Event end time in ns since the unix epoch')]
    events_seen = 0
    __version__ = '0.0.6'

    def get_window_size(self):
        return (2 * self.config['left_event_extension'] +
                self.config['right_event_extension'])

    def compute(self, peaks):
        le = self.config['left_event_extension']
        re = self.config['right_event_extension']

        triggers = peaks[
            (peaks['area'] > self.config['trigger_min_area'])
            & (peaks['n_competing'] >= self.config['trigger_min_competing'])]
        # Join nearby triggers
        t0, t1 = strax.find_peak_groups(
            triggers,
            gap_threshold=le + re + 1,
            left_extension=le,
            right_extension=re)

        result = np.zeros(len(t0), self.dtype)
        result['time'] = t0
        result['endtime'] = t1
        result['event_number'] = np.arange(len(result)) + self.events_seen

        if not result.size > 0:
            print("Found chunk without events?!")

        self.events_seen += len(result)

        return result

