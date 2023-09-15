import numba
import numpy as np
import strax
export, __all__ = strax.exporter()

@export
class CorrectedAreas(strax.Plugin):
    """
    Plugin which applies light collection efficiency maps and electron
    life time to the data.
    Computes the cS1/cS2 for the main/alternative S1/S2 as well as the
    corrected life time.
    Note:
        Please be aware that for both, the main and alternative S1, the
        area is corrected according to the xy-position of the main S2.
        There are now 3 components of cS2s: cs2_top, cS2_bottom and cs2.
        cs2_top and cs2_bottom are corrected by the corresponding maps,
        and cs2 is the sum of the two.
    """
    __version__ = '0.5.1'

    depends_on = ['event_basics', 'event_positions']

    def infer_dtype(self):
        dtype = []
        dtype += strax.time_fields

        for peak_type, peak_name in zip(['', 'alt_'], ['main', 'alternate']):
            # Only apply 
            dtype += [
                (f'{peak_type}cs1', np.float32, f'Corrected area of {peak_name} S1 [PE]'),
                (
                    f'{peak_type}cs1_wo_timecorr', np.float32,
                    f'Corrected area of {peak_name} S1 (before LY correction) [PE]',
                ),
            ]
            names = ['_wo_timecorr', '_wo_picorr', '_wo_elifecorr', '']
            descriptions = ['S2 xy', 'SEG/EE', 'photon ionization', 'elife']
            for i, name in enumerate(names):
                if i == len(names) - 1:
                    description = ''
                elif i == 0:
                    # special treatment for wo_timecorr, apply elife correction
                    description = ' (before ' + ' + '.join(descriptions[i + 1:-1])
                    description += ', after ' + ' + '.join(
                        descriptions[:i + 1] + descriptions[-1:]) + ')'
                else:
                    description = ' (before ' + ' + '.join(descriptions[i + 1:])
                    description += ', after ' + ' + '.join(descriptions[:i + 1]) + ')'
                dtype += [
                    (
                        f'{peak_type}cs2{name}', np.float32,
                        f'Corrected area of {peak_name} S2{description} [PE]',
                    ),
                    (
                        f'{peak_type}cs2_area_fraction_top{name}', np.float32,
                        f'Fraction of area seen by the top PMT array for corrected '
                        f'{peak_name} S2{description}',
                    ),
                ]
        return dtype

    def compute(self, events):
        result = np.zeros(len(events), self.dtype)
        result['time'] = events['time']
        result['endtime'] = events['endtime']

        # S1 corrections depend on the actual corrected event position.
        # We use this also for the alternate S1; for e.g. Kr this is
        # fine as the S1 correction varies slowly.
        event_positions = np.vstack([events['x'], events['y'], events['z']]).T

        for peak_type in ["", "alt_"]:
            result[f"{peak_type}cs1"] = (
                result[f"{peak_type}cs1_wo_timecorr"] / 1) #self.rel_light_yield)

        # now can start doing corrections
        for peak_type in ["", "alt_"]:
            # S2(x,y) corrections use the observed S2 positions
            s2_positions = np.vstack([events[f'{peak_type}s2_x'], events[f'{peak_type}s2_y']]).T

            # collect electron lifetime correction
            # for electron lifetime corrections to the S2s,
            # use drift time computed using the main S1.
            el_string = peak_type + "s2_interaction_" if peak_type == "alt_" else peak_type
            elife_correction = 1 #np.exp(events[f'{el_string}drift_time'] / self.elife)

            # apply electron lifetime correction
            result[f"{peak_type}cs2"] = events[f"{peak_type}s2_area"] * elife_correction
            
        return result
#