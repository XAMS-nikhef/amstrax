import numba
import numpy as np
import strax
export, __all__ = strax.exporter()

# from amstrax.SiPMdata import *
# move this to legacy once you have the new peak_positions.py in amstrax

# @export
# class PeakPositions(strax.Plugin):
#     depends_on = ('peaks', 'peak_classification')
#     rechunk_on_save = False
#     __version__ = '0.0.34'  # .33 for LNLIKE
#     dtype = [
#         ('xr', np.float32,
#          'Interaction x-position'),
#         ('yr', np.float32,
#          'Interaction y-position'),
#         ('r', np.float32,
#          'radial distance'),
#         ('time', np.int64, 'Start time of the peak (ns since unix epoch)'),
#         ('endtime', np.int64, 'End time of the peak (ns since unix epoch)')
#     ]

#     def setup(self):
#         # z position of the in-plane SiPMs
#         z_plane = 10
#         # radius of the cylinder for SiPMs at the side
#         r_cylinder = 22
#         # radius of a SiPM - I assume circular SiPMs with a radius to make the area correspond to a 3x3mm2 square.
#         r_sipm = 1.6925
#         # build geometry
#         geo = GeoParameters(z_plane=z_plane, r_cylinder=r_cylinder, r_sipm=r_sipm)

#         sipm = SiPM(type="plane", position=[0, -15, z_plane], qeff=0.25)
#         geo.add_sipm(sipm)
#         sipm = SiPM(type="plane", position=[-13, -7.5, z_plane], qeff=0.25)
#         geo.add_sipm(sipm)
#         # sipm = SiPM(type="plane", position=[0, 15, z_plane], qeff=0.25)
#         # geo.add_sipm(sipm)
#         sipm = SiPM(type="plane", position=[13, -7.5, z_plane], qeff=0.25)
#         geo.add_sipm(sipm)
#         sipm = SiPM(type="plane", position=[-4, 0, z_plane], qeff=0.25)
#         geo.add_sipm(sipm)
#         sipm = SiPM(type="plane", position=[4, 0, z_plane], qeff=0.25)
#         geo.add_sipm(sipm)
#         sipm = SiPM(type="plane", position=[-13, 7.5, z_plane], qeff=0.25)
#         geo.add_sipm(sipm)
#         sipm = SiPM(type="plane", position=[13, 7.5, z_plane], qeff=0.25)
#         geo.add_sipm(sipm)

#         self.geo = geo

#     def compute(self, peaks):

#         result = np.empty(len(peaks), dtype=self.dtype)

#         if not len(peaks):
#             return result

#         for ix, p in enumerate(peaks):

#             if p['type'] != 2:
#                 continue

#             # if [X] channel is not working
#             k = np.delete(p['area_per_channel'], [2])
#             for i, area in enumerate(k):
#                 self.geo.sipms[i].set_number_of_hits(area)

#             # if all 8 channels are working
#             # for i, area in enumerate(p['area_per_channel']):
#             #     self.geo.sipms[i].set_number_of_hits(area)

#             posrec = Reconstruction(self.geo)
#             pos = posrec.reconstruct_position('CHI2')
#             for key in ['xr', 'yr']:
#                 result[key][ix] = pos[key]

#             for q in ['time', 'endtime']:
#                 result[q] = p[q]

#         result['r'] = (result['xr'] ** 2 + result['yr'] ** 2) ** (1 / 2)
#         return result
