import numba
import numpy as np
import strax
export, __all__ = strax.exporter()

@export
class EventBasics(strax.LoopPlugin):


    """
    TODO 
    """


    rechunk_on_save = False
    __version__ = '0.0.21'
    # Peak Positions temporarily taken out
    # n_competing within peak_basics
    depends_on = ('events',
                  'peak_basics', 'peak_classification',)

    # 'peak_positions') #n_competing

    provides = 'event_basics'


    def infer_dtype(self):
        dtype = [(('Number of peaks in the event',
                   'n_peaks'), np.int32),
                 (('Drift time between main S1 and S2 in ns',
                   'drift_time'), np.int64),
                 ('time', np.int64, 'Event start time in ns since the unix epoch'),
                 ('endtime', np.int64, 'Event end time in ns since the unix epoch')
                 ]
        for i in [1, 2]:
            dtype += [((f'Main S{i} peak index',
                        f's{i}_index'), np.int32),
                      ((f'Main S{i} area (PE), uncorrected',
                        f's{i}_area'), np.float32),
                      ((f'Main S{i} area fraction top',
                        f's{i}_area_fraction_top'), np.float32),
                      ((f'Main S{i} width (ns, 50% area)',
                        f's{i}_range_50p_area'), np.float32),
                      ((f'Main S{i} number of competing peaks',
                        f's{i}_n_competing'), np.int32)]
        # dtype += [(f'x_s2', np.float32,
        #            f'Main S2 reconstructed X position (cm), uncorrected',),
        #           (f'y_s2', np.float32,
        #            f'Main S2 reconstructed Y position (cm), uncorrected',)]
        dtype += [(f's2_largest_other', np.float32,
                   f'Largest other S2 area (PE) in event, uncorrected',),
                  (f's1_largest_other', np.float32,
                   f'Largest other S1 area (PE) in event, uncorrected',),
                  (f'alt_s1_interaction_drift_time', np.float32,
                   f'Drift time with alternative s1',)
                  ]

        return dtype

    def compute_loop(self, event, peaks):
        result = dict(n_peaks=len(peaks))
        if not len(peaks):
            return result

        main_s = dict()
        for s_i in [2, 1]:
            s_mask = peaks['type'] == s_i

            # For determining the main S1, remove all peaks
            # after the main S2 (if there was one)
            # This is why S2 finding happened first
            if s_i == 1 and result[f's2_index'] != -1:
                s_mask &= peaks['time'] < main_s[2]['time']

            ss = peaks[s_mask]
            s_indices = np.arange(len(peaks))[s_mask]

            if not len(ss):
                result[f's{s_i}_index'] = -1
                continue

            main_i = np.argmax(ss['area'])
            # Find largest other signals
            if s_i == 2 and ss['n_competing'][main_i] > 0 and len(ss['area']) > 1:
                s2_second_i = np.argsort(ss['area'])[-2]
                result[f's2_largest_other'] = ss['area'][s2_second_i]

            if s_i == 1 and ss['n_competing'][main_i] > 0 and len(ss['area']) > 1:
                s1_second_i = np.argsort(ss['area'])[-2]
                result[f's1_largest_other'] = ss['area'][s1_second_i]

            result[f's{s_i}_index'] = s_indices[main_i]
            s = main_s[s_i] = ss[main_i]

            for prop in ['area', 'area_fraction_top',
                         'range_50p_area', 'n_competing']:
                result[f's{s_i}_{prop}'] = s[prop]
            # if s_i == 2:
            #     result['x_s2'] = s['xr']
            #     result['y_s2'] = s['yr']

        # Compute a drift time only if we have a valid S1-S2 pairs
        if len(main_s) == 2:
            result['drift_time'] = main_s[2]['time'] - main_s[1]['time']
            # Compute alternative drift time
            if 's1_second_i' in locals():
                result['alt_s1_interaction_drift_time'] = main_s[2]['time'] - ss['time'][
                    s1_second_i]

        return result

