import numba
import numpy as np
import strax
from immutabledict import immutabledict
import amstrax

export, __all__ = strax.exporter()

DEFAULT_POSREC_ALGO = 'corr'

@export
@strax.takes_config(
    strax.Option(
        "channel_map",
        type=immutabledict,
        track=False,
        help="Map of channel numbers to top, bottom and aqmon, to be defined in the context",
    ),
    strax.Option('default_reconstruction_algorithm',
                 default=DEFAULT_POSREC_ALGO,
                 help="default reconstruction algorithm that provides (x,y)"
    )
)
class PeakPositions(strax.Plugin):
    depends_on = ('peaks', 'peak_basics')
    rechunk_on_save = False
    __version__ = '1.2.26'
    dtype = [
        ('x_cgr', np.float32,
         'Interaction x_cgr-position center of gravity'),
        ('y_cgr', np.float32,
         'Interaction y_cgr-position center of gravity'),
        ('r_cgr', np.float32,
         'radial distance from center of gravity'),
        ('x_corr', np.float32,
         'Corrected interaction x-position'),
        ('y_corr', np.float32,
         'Corrected interaction y-position'),
        ('r_corr', np.float32,
         'Corrected radial distance from center'),
        ('x', np.float32,
         'Interaction x-position for the default reconstruction algorithm'),
        ('y', np.float32,
         'Interaction y-position for the default reconstruction algorithm'),
        ('r', np.float32,
         'Interaction r for the default reconstruction algorithm'),
        ('time', np.int64, 'Start time of the peak (ns since unix epoch)'),
        ('endtime', np.int64, 'End time of the peak (ns since unix epoch)')
    ]

    pos_rec_params = amstrax.XAMSConfig(
        default=[
            [212.89988642, -320.95097295, 198.71830514, -46.03953999],
            [211.40051359, -319.83147964, 198.69552524, -46.23585688]
        ],
        help="Parameters for a correction function to go from x_cgr to true x and y_cgr to true y"
    )

    def setup(self):
        
        self.default_reconstruction_algorithm = self.config['default_reconstruction_algorithm']

    def compute(self, peaks):
                
        result = np.empty(len(peaks), dtype=self.dtype)
        result['time'] = peaks['time']
        result['endtime'] = peaks['endtime']
        
        top_sum = peaks['area']*peaks['area_fraction_top']

        # top is going to be (1,4)
        top_map = self.config['channel_map']['top']
        top_indeces = np.arange(top_map[0], top_map[1]+1)

        # a bit convoluted, but necessary to avoid division by zero
        numerator_12 = peaks['area_per_channel'][:,top_indeces[0]]+peaks['area_per_channel'][:,top_indeces[1]]
        numerator_13 = peaks['area_per_channel'][:,top_indeces[0]]+peaks['area_per_channel'][:,top_indeces[2]]
        denominator = top_sum[:]
        
        mask = (denominator != 0)
        
        # f_12 is the fraction of the top area in the top row
        # if top is (1,4) this means that we take the area of channel 1 and 2
        # f_13 is the fraction of the top area in the first column
        # if top is (1,4) this means that we take the area of channel 1 and 3
        f_12 = np.full(len(peaks), np.nan, dtype=np.float32)
        f_13 = np.full(len(peaks), np.nan, dtype=np.float32)
                
        f_12[mask] = numerator_12[mask]/denominator[mask]
        f_13[mask] = numerator_13[mask]/denominator[mask]
        
        result['x_cgr'] = f_12
        result['y_cgr'] = f_13
        result['r_cgr'] = np.sqrt(result['x_cgr']**2+result['y_cgr']**2)

        # correct the x and y cgr positions
        px = self.pos_rec_params[0]
        py = self.pos_rec_params[1]

        rec_function_x = np.poly1d(np.array(px))
        rec_function_y = np.poly1d(np.array(py))
        
        result['x_corr'] = rec_function_x(result['x_cgr'])
        result['y_corr'] = rec_function_y(result['y_cgr'])
        result['r_corr'] = np.sqrt(result['x_corr']**2+result['y_corr']**2)

        # set x, y, z to be the values from the default reconstruction algorithm
        algo = self.default_reconstruction_algorithm
        result['x'] = result[f'x_{algo}']
        result['y'] = result[f'y_{algo}']
        result['r'] = result[f'r_{algo}']
        
        return result
