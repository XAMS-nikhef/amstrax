import numba
import numpy as np
import strax
export, __all__ = strax.exporter()

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
