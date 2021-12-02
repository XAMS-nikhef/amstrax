import strax

import numpy as np

from amstrax.common import pax_file, get_resource, get_elife, first_sr1_run
from amstrax.itp_map import InterpolatingMap
from .SiPMdata import *

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
        # TODO: someday investigate if/why loopplugin doesn't give
        # anything if events do not contain peaks..
        # Likely this has been resolved in 6a2cc6c


@export
class EventBasics(strax.LoopPlugin):
    rechunk_on_save = False
    __version__ = '0.0.21'
    # Peak Positions temporarily taken out
    # n_competing within peak_basics
    depends_on = ('events',
                  'peak_basics', 'peak_classification',)

    # 'peak_positions') #n_competing

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


@export
class EventPositions(strax.LoopPlugin):
    depends_on = ('events', 'event_basics', 'peaks', 'peak_classification')
    rechunk_on_save = False
    dtype = [
        ('xr', np.float32,
         'Interaction x-position'),
        ('yr', np.float32,
         'Interaction y-position'),
        ('time', np.int64, 'Event start time in ns since the unix epoch'),
        ('endtime', np.int64, 'Event end time in ns since the unix epoch')
    ]
    __version__ = '0.0.4'

    def setup(self):
        # z position of the in-plane SiPMs
        z_plane = 10
        # radius of the cyinder for SiPMs at the side
        r_cylinder = 22
        # radius of a SiPM - I assume circular SiPMs with a radius to make the area correspond to a 3x3mm2 square.
        r_sipm = 1.6925
        # build geometry
        geo = GeoParameters(z_plane=z_plane, r_cylinder=r_cylinder, r_sipm=r_sipm)

        sipm = SiPM(type="plane", position=[0, -15, z_plane], qeff=0.25)
        geo.add_sipm(sipm)
        sipm = SiPM(type="plane", position=[-13, -7.5, z_plane], qeff=0.25)
        geo.add_sipm(sipm)
        sipm = SiPM(type="plane", position=[13, -7.5, z_plane], qeff=0.25)
        geo.add_sipm(sipm)
        sipm = SiPM(type="plane", position=[-4, 0, z_plane], qeff=0.25)
        geo.add_sipm(sipm)
        sipm = SiPM(type="plane", position=[4, 0, z_plane], qeff=0.25)
        geo.add_sipm(sipm)
        sipm = SiPM(type="plane", position=[-13, 7.5, z_plane], qeff=0.25)
        geo.add_sipm(sipm)
        sipm = SiPM(type="plane", position=[13, 7.5, z_plane], qeff=0.25)
        geo.add_sipm(sipm)

        self.geo = geo

    def compute_loop(self, events, peaks):
        result = dict()

        if not len(peaks):
            return result

        s2_index = events['s2_index']
        if s2_index == -1 or s2_index > len(peaks[(peaks['type'] == 2)]) - 1:
            return result

        s2_peak = peaks[(peaks['type'] == 2)][s2_index]
        for i, area in enumerate(s2_peak['area_per_channel'][:7]):
            self.geo.sipms[i].set_number_of_hits(area)

        posrec = Reconstruction(self.geo)
        pos = posrec.reconstruct_position('LNLIKE')
        for key in ['xr', 'yr']:
            result[key] = pos[key]
        return result


#
#
# @export
# @strax.takes_config(
#     strax.Option(
#         's1_relative_lce_map',
#         help="S1 relative LCE(x,y,z) map",
#         default_by_run=[
#             (0, pax_file('XENON1T_s1_xyz_lce_true_kr83m_SR0_pax-680_fdc-3d_v0.json')),  # noqa
#             (first_sr1_run, pax_file('XENON1T_s1_xyz_lce_true_kr83m_SR1_pax-680_fdc-3d_v0.json'))]),  # noqa
#     strax.Option(
#         's2_relative_lce_map',
#         help="S2 relative LCE(x, y) map",
#         default_by_run=[
#             (0, pax_file('XENON1T_s2_xy_ly_SR0_24Feb2017.json')),
#             (170118_1327, pax_file('XENON1T_s2_xy_ly_SR1_v2.2.json'))]),
#    strax.Option(
#         'elife_file',
#         default='https://raw.githubusercontent.com/XENONnT/strax_auxiliary_files/master/elife.npy',
#         help='link to the electron lifetime'))
# class CorrectedAreas(strax.Plugin):
#     depends_on = ['event_basics', 'event_positions']
#     dtype = [('cs1', np.float32, 'Corrected S1 area (PE)'),
#              ('cs2', np.float32, 'Corrected S2 area (PE)')]
#
#     def setup(self):
#         self.s1_map = InterpolatingMap(
#             get_resource(self.config['s1_relative_lce_map']))
#         self.s2_map = InterpolatingMap(
#             get_resource(self.config['s2_relative_lce_map']))
#         # self.elife = get_elife(self.run_id,self.config['elife_file'])
#         self.elife = 632e5
#
#     def compute(self, events):
#         event_positions = np.vstack([events['x'], events['y'], events['z']]).T
#         s2_positions = np.vstack([events['x_s2'], events['y_s2']]).T
#         lifetime_corr = np.exp(
#             events['drift_time'] / self.elife)
#
#         return dict(
#             cs1=events['s1_area'] / self.s1_map(event_positions),
#             cs2=events['s2_area'] * lifetime_corr / self.s2_map(s2_positions))
#
# @export
# @strax.takes_config(
#     strax.Option(
#         'g1',
#         help="S1 gain in PE / photons produced",
#         default_by_run=[(0, 0.1442),
#                         (first_sr1_run, 0.1426)]),
#     strax.Option(
#         'g2',
#         help="S2 gain in PE / electrons produced",
#         default_by_run=[(0, 11.52),
#                         (first_sr1_run, 11.55)]),
#     strax.Option(
#         'lxe_w',
#         help="LXe work function in quanta/eV",
#         default=13.7e-3),
# )
# class EnergyEstimates(strax.Plugin):
#     depends_on = ['corrected_areas']
#     dtype = [
#         ('e_light', np.float32, 'Energy in light signal (keV)'),
#         ('e_charge', np.float32, 'Energy in charge signal (keV)'),
#         ('e_ces', np.float32, 'Energy estimate (keV_ee)')]
#
#     def compute(self, events):
#         w = self.config['lxe_w']
#         el = w * events['cs1'] / self.config['g1']
#         ec = w * events['cs2'] / self.config['g2']
#         return dict(e_light=el,
#                     e_charge=ec)
#
@export
class EventInfo(strax.MergeOnlyPlugin):
    depends_on = ['events',
                  'event_basics',
                  'event_positions',
                  # 'energy_estimates',
                  ]
    rechunk_on_save = True
    provides = 'event_info'
    save_when = strax.SaveWhen.ALWAYS
    __version__ = '0.0.2'
