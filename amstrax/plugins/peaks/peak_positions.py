import numba
import numpy as np
import strax
from immutabledict import immutabledict
export, __all__ = strax.exporter()


@export
@strax.takes_config(
        strax.Option(
        "channel_map",
        type=immutabledict,
        track=False,
        help="Map of channel numbers to top, bottom and aqmon, to be defined in the context",
    ),
)
class PeakPositions(strax.Plugin):
    depends_on = ('peaks', 'peak_basics')
    rechunk_on_save = False
    __version__ = '1.0'
    dtype = [
        ('x_cgr', np.float32,
         'Interaction x-position center of gravity'),
        ('y_cgr', np.float32,
         'Interaction y-position center of gravity'),
        ('r_cgr', np.float32,
         'radial distance from center of gravity'),
        ('time', np.int64, 'Start time of the peak (ns since unix epoch)'),
        ('endtime', np.int64, 'End time of the peak (ns since unix epoch)')
    ]


    def compute(self, peaks):

        result = np.empty(len(peaks), dtype=self.dtype)
        result['time'] = peaks['time']
        result['endtime'] = peaks['endtime']
        
        top_sum = peaks['area']*peaks['area_fraction_top']

        # top is going to be (1,4)
        top_map = self.config['channel_map']['top']
        top_indeces = np.arange(top_map[0], top_map[1]+1)

        # f_12 is the fraction of the top area in the top row
        # if top is (1,4) this means that we take the area of channel 1 and 2
        f_12 = (peaks['area_per_channel'][:,top_indeces[0]]+peaks['area_per_channel'][:,top_indeces[1]])/top_sum[:]
        # f_13 is the fraction of the top area in the first column
        # if top is (1,4) this means that we take the area of channel 1 and 3
        f_13 = (peaks['area_per_channel'][:,top_indeces[0]]+peaks['area_per_channel'][:,top_indeces[2]])/top_sum[:]
        
        result['x_cgr'] = f_12
        result['y_cgr'] = f_13
        result['r_cgr'] = np.sqrt(result['x']**2+result['y']**2)

        return result
