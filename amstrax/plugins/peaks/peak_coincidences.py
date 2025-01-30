import numba
import numpy as np
import strax
from immutabledict import immutabledict
export, __all__ = strax.exporter()

@export
@strax.takes_config(
        strax.Option(
        "max_delay",
        default=100,
        help="Maximum allowed time difference between XAMS peaks and external peaks to count as a match, in ns",
        ),
        strax.Option(
        "absorption_peak_delta",
        default=65,
        help="Sets the window as [511 - delta, 511 + delta] in keV for the allowed energies that count as an absorption event of a 511 keV photon in the external detector",
        ),
)
class PeakCoincidences(strax.OverlapWindowPlugin):
    """
    Some runs are taken with a na-22 source. This source emits two 511keV photons in exactly opposite directions.
    For these runs, we placed an external detector next to XAMS that is good at detecting 511keV photons,
    which allows us to tag the XAMS peaks that are coincident with the external detector.
    This plugin provides a bool 'is_coinc' that is True if the peak is coincident with that external detector.
    """

    provides = ('peak_coincidences',)
    depends_on = ('peaks', 'peaks_ext',)
    data_kind = "peaks"
    
    rechunk_on_save = False
    __version__ = '1.0'
    
    dtype = [
        ('time', np.int64, 'Start time of the peak (ns since unix epoch)'),
        ('endtime', np.int64, 'End time of the peak (ns since unix epoch)'),
        ('is_coinc', np.bool_, 'Whether a peak has an external match or not'),
    ]
    
    def get_window_size(self):
        """Sets the overlap window to be twice the maximum distance between two matched peaks"""
        return int(2 * self.config['max_delay'])
    
    def compute(self, peaks, peaks_ext):
        result = np.empty(len(peaks), dtype=self.dtype)
        result['time'] = peaks['time']
        result['endtime'] = strax.endtime(peaks)
        
        na22_peaks = peaks_ext[(peaks_ext['area'] > 511 - self.config['absorption_peak_delta']) & (peaks_ext['area'] < 511 + self.config['absorption_peak_delta'])]
        result['is_coinc'] = self.matching_peaks(peaks['time'], na22_peaks['time'], self.config['max_delay'])
        
        return result

    @staticmethod
    @numba.jit(nopython=True, nogil=True, cache=False)
    def matching_peaks(XAMS_times, ext_times, max_delay):
        """
        Pairs peaks (XAMS) and peaks_ext (external NaI detector) signals with a 2 index method.
        
        NOTE: This method assumes that the external time is always LATER than the XAMS time.
        So ext_peak_time > peak_time. It might be possible that this will be the other way around 
        if the experiment changes, in which case the time differences might need to be swapped.
        """

        num_XAMS = len(XAMS_times)
        num_ext = len(ext_times)

        # booleans to keep track of which XAMS peaks have an associated external peak
        matches = np.zeros(num_XAMS, dtype=np.bool_)

        i, j = 0, 0

        # check per external peak if there is a XAMS peak
        while i < num_XAMS - 2 and j < num_ext - 2:
            time_diff = ext_times[j] - XAMS_times[i]
            
            if time_diff < 0:
                # move to the next external peak
                j += 1

            elif time_diff <= max_delay:
                matches[i] = True
                
                # pair found: move both to the next peak
                i += 1
                j += 1
            
            else:
                # keep on moving to the next XAMS peak until we are in front again
                while ext_times[j] - XAMS_times[i] > max_delay and not i >= num_XAMS - 2:
                    i += 1
            
        return matches