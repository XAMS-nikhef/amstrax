import numba
import numpy as np
import strax
export, __all__ = strax.exporter()

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
