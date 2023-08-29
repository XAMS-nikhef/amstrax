import numba
import numpy as np
import strax
export, __all__ = strax.exporter()

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