import numba
import numpy as np
import strax
from immutabledict import immutabledict
from strax.processing.general import _touching_windows

from amstrax.SiPMdata import *
from .radon_pulse_processing import HITFINDER_OPTIONS

export, __all__ = strax.exporter()

@export
@strax.takes_config(
    strax.Option('peak_gap_threshold', default=3000, infer_type=False,
                 help="No hits for this many ns triggers a new peak"),
    strax.Option('peak_left_extension', default=1000, infer_type=False,
                 help="Include this many ns left of hits in peaks"),
    strax.Option('peak_right_extension', default=1000, infer_type=False,
                 help="Include this many ns right of hits in peaks"),
    strax.Option('peak_min_pmts', default=1, infer_type=False,
                 help="Minimum contributing PMTs needed to define a peak"), 
    strax.Option('peak_split_gof_threshold',
                 # See https://xe1t-wiki.lngs.infn.it/doku.php?id=
                 # xenon:xenonnt:analysis:strax_clustering_classification
                 # #natural_breaks_splitting
                 # for more information
                 default=(
                     None,  # Reserved
                     ((0.5, 1.0), (6.0, 0.4)),
                     ((2.5, 1.0), (5.625, 0.4))), infer_type=False,
                 help='Natural breaks goodness of fit/split threshold to split '
                      'a peak. Specify as tuples of (log10(area), threshold).'),
    strax.Option('peak_split_filter_wing_width', default=70, infer_type=False,
                 help='Wing width of moving average filter for '
                      'low-split natural breaks'),
    strax.Option('peak_split_min_area', default=40., infer_type=False,
                 help='Minimum area to evaluate natural breaks criterion. '
                      'Smaller peaks are not split.'),
    strax.Option('peak_split_iterations', default=20, infer_type=False,
                 help='Maximum number of recursive peak splits to do.'),
    strax.Option('diagnose_sorting', track=False, default=False,
                 help="Enable runtime checks for sorting and disjointness"),
    strax.Option('saturation_correction_on', default=True, infer_type=False,
                 help='On off switch for saturation correction'),
    strax.Option('saturation_reference_length', default=100, infer_type=False,
                 help="Maximum number of reference sample used "
                      "to correct saturated samples"),
    strax.Option('saturation_min_reference_length', default=20, infer_type=False,
                 help="Minimum number of reference sample used "
                      "to correct saturated samples"),
    strax.Option('n_tpc_pmts', type=int,
                 help='Number of TPC PMTs'),
    strax.Option('peak_min_area', default=1, infer_type=False,
                 help="Minimum contributing PMTs needed to define a peak"),
    strax.Option('single_channel_peaks', default=False,
                 help='Whether single-channel peaks should be reported'),
    strax.Option('peak_split_min_height', default=25,
                 help="Minimum height in PE above a local sum waveform"
                      "minimum, on either side, to trigger a split"),
    strax.Option('peak_split_min_ratio', default=4,
                 help="Minimum ratio between local sum waveform"
                      "minimum and maxima on either side, to trigger a split"),
    strax.Option('pmt_channel', default=0,
                 help="PMT channel for splitting pmt and sipms"), 
    strax.Option('peak_max_duration', default=int(10e6), infer_type=False,
                 help="Maximum duration [ns] of a peaklet"),
    strax.Option('channel_map', track=False, type=immutabledict,
                 help="immutabledict mapping subdetector to (min, max) "
                      "channel number."),
    *HITFINDER_OPTIONS,
)
class RadonPeaks(strax.Plugin):
    """
    Split records into:
        - peaks
        - lone_hits
    
    Peaks are calculated from records with the following method:
        1. Hit finding 
        2. Peak finding 
        3. Peak splitting using the natural breaks algorithm in strax
        4. Compute digital waveform

    Lone hits are all hits which are outside any peak. The area of
    the lone_hits includes the left and right hit extension, except the
    extension overlaps with any peak or other hits.
    """
    __version__ = '0.1.78'

    parallel = 'process'
    rechunk_on_save = True
    compressor = 'zstd'

    depends_on = ('radon_records',)

    provides = ('radon_peaks','radon_lone_hits')
    data_kind = dict(radon_peaks='radon_peaks',
                    radon_lone_hits='radon_lone_hits')

    def infer_dtype(self):
        return dict(radon_peaks=strax.peak_dtype(
                        n_channels=self.config['n_xamsl_channel ']),
                    radon_lone_hits=strax.hit_dtype)

    def compute(self, radon_records, start, end):
        # Hit finding
        hits = strax.find_hits(radon_records,min_amplitude=self.config['hit_threshold'])
        
        self.to_pe = np.ones(8)

        # Remove hits in zero-gain channels
        # they should not affect the clustering!
        hits = hits[self.to_pe[hits['channel']] != 0]

        hits = strax.sort_by_time(hits)
        #  Use peaklet gap threshold for initial clustering
        # based on gaps between hits
        peaks = strax.find_peaks(
            hits, self.to_pe,
            gap_threshold=self.config['peak_gap_threshold'],
            left_extension=self.config['peak_left_extension'],
            right_extension=self.config['peak_right_extension'],
            min_area=self.config['peak_min_area'],
            min_channels=self.config['peak_min_pmts'],
            result_dtype=strax.peak_dtype(n_channels=4),
            max_duration=self.config['peak_max_duration'],
        )

        # Make sure peaks don't extend out of the chunk boundary
        # This should be very rare in normal data due to the ADC pretrigger
        # window.
        self.clip_peaklet_times(peaks, start, end)

        # Get hits outside peaklets, and store them separately.
        # fully_contained is OK provided gap_threshold > extension,
        # which is asserted inside strax.find_peaks.
        is_lone_hit = strax.fully_contained_in(hits, peaks) == -1
        lone_hits = hits[is_lone_hit]
        strax.integrate_lone_hits(
            lone_hits, radon_records, peaks,
            save_outside_hits=(self.config['peak_left_extension'],
                               self.config['peak_right_extension']),
            n_channels=len(self.to_pe))

        # Compute basic peak properties -- needed before natural breaks
        hits = hits[~is_lone_hit]
        # Define regions outside of peaks such that _find_hit_integration_bounds
        # is not extended beyond a peak.
        outside_peaks = self.create_outside_peaks_region(peaks, start, end)
        strax.find_hit_integration_bounds(
            hits, outside_peaks, radon_records,
            save_outside_hits=(self.config['peak_left_extension'],
                               self.config['peak_right_extension']),
            n_channels=len(self.to_pe),
            allow_bounds_beyond_records=True,
        )

        # Transform hits to hitlets for naming conventions. A hit refers
        # to the central part above threshold a hitlet to the entire signal
        # including the left and right extension.
        # (We are not going to use the actual hitlet data_type here.)
        hitlets = hits
        del hits

        hitlet_time_shift = (hitlets['left'] - hitlets['left_integration']) * hitlets['dt']
        hitlets['time'] = hitlets['time'] - hitlet_time_shift
        hitlets['length'] = (hitlets['right_integration'] - hitlets['left_integration'])
        hitlets = strax.sort_by_time(hitlets)
        rlinks = strax.record_links(radon_records)

        strax.sum_waveform(peaks, hitlets, radon_records, rlinks, self.to_pe)

        strax.compute_widths(peaks)

        # Split peaks using low-split natural breaks;
        # see https://github.com/XENONnT/straxen/pull/45
        # and https://github.com/AxFoundation/strax/pull/225
        peaks = strax.split_peaks(
            peaks, hitlets, radon_records, rlinks, self.to_pe,
            algorithm='natural_breaks',
            threshold=self.natural_breaks_threshold,
            split_low=True,
            filter_wing_width=self.config['peak_split_filter_wing_width'],
            min_area=self.config['peak_split_min_area'],
            do_iterations=self.config['peak_split_iterations'])

        # Saturation correction using non-saturated channels
        # TODO

        # Compute tight coincidence level. TODO
        if self.config['diagnose_sorting'] and len(r):
            assert np.diff(r['time']).min(initial=1) >= 0, "Records not sorted"
            assert np.diff(hitlets['time']).min(initial=1) >= 0, "Hits/Hitlets not sorted"
            assert np.all(peaks['time'][1:]
                          >= strax.endtime(peaks)[:-1]), "Peaks not disjoint"    

        # Update nhits of peaks:
        counts = strax.touching_windows(hitlets, peaks)
        counts = np.diff(counts, axis=1).flatten()
        peaks['n_hits'] = counts
        return dict(radon_peaks=peaks,
                    radon_lone_hits=lone_hits)

    def natural_breaks_threshold(self, peaks):
        rise_time = -peaks['area_decile_from_midpoint'][:, 1]

        # This is ~1 for an clean S2, ~0 for a clean S1,
        # and transitions gradually in between.
        f_s2 = 8 * np.log10(rise_time.clip(1, 1e5) / 100)
        f_s2 = 1 / (1 + np.exp(-f_s2))

        log_area = np.log10(peaks['area'].clip(1, 1e7))
        thresholds = self.config['peak_split_gof_threshold']
        return (
            f_s2 * np.interp(
                log_area,
                *np.transpose(thresholds[2]))
            + (1 - f_s2) * np.interp(
                log_area,
                *np.transpose(thresholds[1])))

    @staticmethod
    @numba.njit(nogil=True, cache=True)
    def clip_peaklet_times(peaklets, start, end):
        for p in peaklets:
            if p['time'] < start:
                p['time'] = start
            if strax.endtime(p) > end:
                p['length'] = (end - p['time']) // p['dt']

    @staticmethod
    def create_outside_peaks_region(peaklets, start, end):
        """
        Creates time intervals which are outside peaks.

        :param peaklets: Peaklets for which intervals should be computed.
        :param start: Chunk start
        :param end: Chunk end
        :return: array of strax.time_fields dtype.
        """
        if not len(peaklets):
            return np.zeros(0, dtype=strax.time_fields)
        
        outside_peaks = np.zeros(len(peaklets) + 1,
                                 dtype=strax.time_fields)
        
        outside_peaks[0]['time'] = start
        outside_peaks[0]['endtime'] = peaklets[0]['time']
        outside_peaks[1:-1]['time'] = strax.endtime(peaklets[:-1])
        outside_peaks[1:-1]['endtime'] = peaklets['time'][1:]
        outside_peaks[-1]['time'] = strax.endtime(peaklets[-1])
        outside_peaks[-1]['endtime'] = end
        return outside_peaks

@numba.jit(nopython=True, nogil=True, cache=True)
def peak_saturation_correction(records, rlinks, peaks, hitlets, to_pe,
                               reference_length=100,
                               min_reference_length=20,
                               use_classification=False,
                               ):
    """Correct the area and per pmt area of peaks from saturation
    :param records: Records
    :param rlinks: strax.record_links of corresponding records.
    :param peaks: Peaklets / Peaks
    :param hitlets: Hitlets found in records to build peaks.
        (Hitlets are hits including the left/right extension)
    :param to_pe: adc to PE conversion (length should equal number of PMTs)
    :param reference_length: Maximum number of reference sample used
    to correct saturated samples
    :param min_reference_length: Minimum number of reference sample used
    to correct saturated samples
    :param use_classification: Option of using classification to pick only S2
    """

    if not len(records):
        return
    if not len(peaks):
        return

    # Search for peaks with saturated channels
    mask = peaks['n_saturated_channels'] > 0
    if use_classification:
        mask &= peaks['type'] == 2
    peak_list = np.where(mask)[0]
    # Look up records that touch each peak
    record_ranges = _touching_windows(
        records['time'],
        strax.endtime(records),
        peaks[peak_list]['time'],
        strax.endtime(peaks[peak_list]))

    # Create temporary arrays for calculation
    dt = records[0]['dt']
    n_channels = len(peaks[0]['saturated_channel'])
    len_buffer = np.max(peaks['length'] * peaks['dt']) // dt + 1
    max_nrecord = len_buffer // len(records[0]['data']) + 1

    # Buff the sum wf [pe] of non-saturated channels
    b_sumwf = np.zeros(len_buffer, dtype=np.float32)
    # Buff the records 'data' [ADC] in saturated channels
    b_pulse = np.zeros((n_channels, len_buffer), dtype=np.int16)
    # Buff the corresponding record index of saturated channels
    b_index = np.zeros((n_channels, max_nrecord), dtype=np.int64)

    # Main
    for ix, peak_i in enumerate(peak_list):
        # reset buffers
        b_sumwf[:] = 0
        b_pulse[:] = 0
        b_index[:] = -1

        p = peaks[peak_i]
        channel_saturated = p['saturated_channel'] > 0

        for record_i in range(record_ranges[ix][0], record_ranges[ix][1]):
            r = records[record_i]
            r_slice, b_slice = strax.overlap_indices(
                r['time'] // dt, r['length'],
                p['time'] // dt, p['length'] * p['dt'] // dt)

            ch = r['channel']
            if channel_saturated[ch]:
                b_pulse[ch, slice(*b_slice)] += r['data'][slice(*r_slice)]
                b_index[ch, np.argmin(b_index[ch])] = record_i
            else:
                b_sumwf[slice(*b_slice)] += r['data'][slice(*r_slice)] \
                    * to_pe[ch]

        _peak_saturation_correction_inner(
            channel_saturated, records, p,
            to_pe, b_sumwf, b_pulse, b_index,
            reference_length, min_reference_length)

        # Back track sum wf downsampling
        peaks[peak_i]['length'] = p['length'] * p['dt'] / dt
        peaks[peak_i]['dt'] = dt

    strax.sum_waveform(peaks, hitlets, records, rlinks, to_pe, peak_list)
    return peak_list


@numba.jit(nopython=True, nogil=True, cache=True)
def _peak_saturation_correction_inner(channel_saturated, records, p,
                                      to_pe, b_sumwf, b_pulse, b_index,
                                      reference_length=100,
                                      min_reference_length=20,
                                      ):
    """Would add a third level loop in peak_saturation_correction
    Which is not ideal for numba, thus this function is written
    :param channel_saturated: (bool, n_channels)
    :param p: One peak/peaklet
    :param to_pe: adc to PE conversion (length should equal number of PMTs)
    :param b_sumwf, b_pulse, b_index: Filled buffers
    """
    dt = records['dt'][0]
    n_channels = len(channel_saturated)

    for ch in range(n_channels):
        if not channel_saturated[ch]:
            continue
        b = b_pulse[ch]
        r0 = records[b_index[ch][0]]

        # Define the reference region as reference_length before the first saturation point
        # unless there are not enough samples
        bl = np.inf
        for record_i in b_index[ch]:
            if record_i == -1:
                break
            bl = min(bl, records['baseline'][record_i])

        s0 = np.argmax(b >= np.int16(bl))
        ref = slice(max(0, s0-reference_length), s0)

        if (b[ref] * to_pe[ch] > 1).sum() < min_reference_length:
            # the pulse is saturated, but there are not enough reference samples to get a good ratio
            # This actually distinguished between S1 and S2 and will only correct S2 signals
            continue
        if (b_sumwf[ref] > 1).sum() < min_reference_length:
            # the same condition applies to the waveform model
            continue
        if np.sum(b[ref]) * to_pe[ch] / np.sum(b_sumwf[ref]) > 1:
            # The pulse is saturated, but insufficient information is available in the other channels
            # to reliably reconstruct it
            continue

        scale = np.sum(b[ref]) / np.sum(b_sumwf[ref])

        # Loop over the record indices of the saturated channel (saved in b_index buffer)
        for record_i in b_index[ch]:
            if record_i == -1:
                break
            r = records[record_i]
            r_slice, b_slice = strax.overlap_indices(
                r['time'] // dt, r['length'],
                p['time'] // dt + s0,  p['length'] * p['dt'] // dt - s0)

            if r_slice[1] == r_slice[0]:  # This record proceeds saturation
                continue
            b_slice = b_slice[0] + s0, b_slice[1] + s0

            # First is finding the highest point in the desaturated record
            # because we need to bit shift the whole record if it exceeds int16 range
            apax = scale * max(b_sumwf[slice(*b_slice)])

            if np.int32(apax) >= 2**15:  # int16(2**15) is -2**15
                bshift = int(np.floor(np.log2(apax) - 14))

                tmp = r['data'].astype(np.int32)
                tmp[slice(*r_slice)] = b_sumwf[slice(*b_slice)] * scale

                r['area'] = np.sum(tmp)  # Auto covert to int64
                r['data'][:] = np.right_shift(tmp, bshift)
                r['amplitude_bit_shift'] += bshift
            else:
                r['data'][slice(*r_slice)] = b_sumwf[slice(*b_slice)] * scale
                r['area'] = np.sum(r['data'])


@numba.njit(cache=True, nogil=True)
def hit_max_sample(records, hits):
    """Return the index of the maximum sample for hits"""
    result = np.zeros(len(hits), dtype=np.int16)
    for i, h in enumerate(hits):
        r = records[h['record_i']]
        w = r['data'][h['left']:h['right']]
        result[i] = np.argmax(w)
    return result


@export
@strax.takes_config(
    # PMT pulse processing options
    strax.Option(
        'save_outside_hits',
        default=(3, 20),
        help='Save (left, right) samples besides hits; cut the rest'),
        *HITFINDER_OPTIONS,
)
class RadonHits(strax.Plugin):
    """
    Find hits using the find_hits algorithm in strax.
    """
    __version__ = '0.0.7'

    parallel = 'False'
    rechunk_on_save = False
    depends_on = 'radon_records'
    data_kind = 'radon_peaks'
    dtype = strax.hit_dtype

    def compute(self, radon_records):
        hits = strax.find_hits(radon_records, min_amplitude=self.config['hit_threshold'])
        return hits