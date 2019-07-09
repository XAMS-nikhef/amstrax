# Code mostly stolen from the straxen github
# mongo.py

"""Convert mongo database documents to flat records format
"""
import numpy as np
import os
import glob
from pymongo import MongoClient
import snappy
import numba

import strax

export, __all__ = strax.exporter()

def records_needed(pulse_length, samples_per_record):
    return np.ceil(pulse_length / samples_per_record).astype(np.int)

@numba.jit(nopython=True, nogil=True, cache=True)
def decode_control_word(n0, n1):
    '''
    Read the control word used in ZLE data (see CAEN V1724 manual)
    WARNING: untested for stretches of > 32,768 samples
    '''
    # The 32-bit control word is split into two numbers, which is why the order is flipped (little-endian numbers)
    if n1 < 0:
        # This means the first bit is 1, signaling real data coming up
        n1 = - n1 - 32768
        is_zle = False
    else:
        is_zle = True
        
    if n0 < 0:
        # this means the first bit is a 1, which should indicate 2**15
        n0 = - n0
        print('[mongo_interface] Warning: very long waveform stretch detected.')
    n_samples = 2 * (n0 + n1 * 2**16)
    # Warning only in nopython mode
    # if n1 > 0:
        # print('[mongo_interface] Warning: very long waveform stretch detected. Number of samples:', n_samples)
    return is_zle, n_samples

@numba.jit(nopython=True, nogil=True, cache=True)
def find_pulse_locations(d, zle=True):
    '''
    Reads ZLE data control words and returns a list with the pulse start samples and the pulse lenght.
    '''
    
    if not zle:
        # If no ZLE: the whole waveform is a single pulse (trivial)
        return [0], [len(d)]
    # Skip first word for unknown reason
    # probably encodes length of entire pulse
    
    pulse_start_samples = []
    pulse_lengths = []
    
    i = 2
    sample_index = 0
    while i< len(d):
        print(d[i],d[i+1])
        is_zle, n_samples = decode_control_word(d[i], d[i+1])
        if not is_zle:
            pulse_start_samples.append(i + 2)
            pulse_lengths.append(n_samples)
            i += n_samples
        sample_index += n_samples    
        i += 2 # move to next control word
    return pulse_start_samples, pulse_lengths

@numba.jit(nopython=True, nogil=True, cache=True)
def fill_records(records, d, pulse_start_samples, pulse_lengths, n_records_list, time_offset, samples_per_record, invert, dt):
    ''' Fill the record array with record-by-record data for the pulses in d
    '''
    output_record_index = 0  # Record offset in data
    for start, length, n_records in zip(pulse_start_samples, pulse_lengths, n_records_list):
        for rec_i in range(n_records):
            r = records[output_record_index]
            r['time'] = (time_offset
                         + start * dt # TODO remove hard coding here
                         + rec_i * samples_per_record * dt)
            r['pulse_length'] = length
            r['record_i'] = rec_i


            # How much are we storing in this record?
            if rec_i != n_records - 1:
                # There's more chunks coming, so we store a full chunk
                n_store = samples_per_record
                assert length > samples_per_record * (rec_i + 1)
            else:
                # Just enough to store the rest of the data
                # Note it's not p.length % samples_per_record!!!
                # (that would be zero if we have to store a full record)
                n_store = length - samples_per_record * rec_i

            assert 0 <= n_store <= samples_per_record
            r['length'] = n_store

            offset = rec_i * samples_per_record + start
            if invert:
                r['data'][:n_store] = - d[offset:offset + n_store]
            else:
                r['data'][:n_store] = d[offset:offset + n_store]
            output_record_index += 1
    return records


@export
# @numba.jit(nopython=True, nogil=True, cache=True)
def mongo_to_records(collection_name,
                     samples_per_record=strax.DEFAULT_RECORD_LENGTH,
                     events_per_chunk=1000,
                     invert_channels = [8,9,10,11,12,13,14,15],
                     zle_channels = [8,9,10,11,12,13,14,15],
                     # invert_channels = [0,1,2,3,4,5,6,7],
                     # zle_channels = [0,1,2,3,4,5,6,7],
                    ):
    """Return pulse records array from mongo database
    """

    results = []

    # Do not know what this does so will keep
    def finish_results():
        nonlocal results
        records = np.concatenate(results)
        # In strax data, records are always stored
        # sorted, baselined and integrated
        records = strax.sort_by_time(records)
        strax.baseline(records)
        strax.integrate(records)
        results = []
        return records

    client = MongoClient()
    dbname = 'xamsdata0'
    cursor = client[dbname][collection_name].find({})

    for i,doc in enumerate(cursor):
        d = np.frombuffer(snappy.decompress(doc['data']), dtype='<i2')

        # Extract channel- or digitizer-dependent properties
        channel = doc['channel'] + int(doc['module'] == 1724) * 8
        zle = True if channel in zle_channels else False
        invert = True if channel in invert_channels else False
        dt = 10 if doc['module'] == 1724 else 2

        # get arrays containing the pulse-by-pulse properties
        print(channel, zle)
        pulse_start_samples, pulse_lengths = find_pulse_locations(d, zle=zle)
        n_records_list = records_needed(np.array(pulse_lengths),
                                             samples_per_record)

        # Build record array for this document data
        n_records_tot = n_records_list.sum()
        records = np.zeros(n_records_tot,
                           dtype=strax.record_dtype(samples_per_record))
        # These are the same for this whole pulse
        records['channel'] = channel
        records['dt'] = dt
        if doc['module'] == 1724:
            pulse_time_offset = doc['time'] * 10
        elif doc['module'] == 1730:
            pulse_time_offset = doc['time'] * 8
        # Heavy lifting in jit-ed loop
        if len(pulse_start_samples):
            records = fill_records(records, d, pulse_start_samples, pulse_lengths, n_records_list,
                               pulse_time_offset, samples_per_record, invert, dt)
        else:
            continue
        results.append(records)
        if len(results) >= events_per_chunk:
            yield finish_results()



@export
@strax.takes_config(
    strax.Option('collection_name', default='', track=False,
                 help="Collection used, example: '190124_110558'"),
    strax.Option('events_per_chunk', default=1000, track=False,
                 help="Number of events to yield per chunk",),
    # strax.Option('invert_channels', default=[8, 9, 10, 11, 12, 13, 14, 15], track=False,
    #              help="List containing the channel numbers to invert",),
    # strax.Option('zle_channels', default=[8, 9, 10, 11, 12, 13, 14, 15], track=False,
    #              help="List containing the channel numbers that have ZLE enabled",),
    strax.Option('invert_channels', default=[8,9], track=False,
                 help="List containing the channel numbers to invert",),
    strax.Option('zle_channels', default=[8,9], track=False,
                 help="List containing the channel numbers that have ZLE enabled",),
)
class RecordsFromMongo(strax.Plugin):
    provides = 'raw_records'
    data_kind = 'raw_records'
    depends_on = tuple()
    dtype = strax.record_dtype()
    parallel = False
    rechunk_on_save = False

    def iter(self, *args, **kwargs):
            yield from mongo_to_records(
                self.config['collection_name'],
                events_per_chunk=self.config['events_per_chunk'],
                invert_channels = self.config['invert_channels'],
                zle_channels = self.config['zle_channels']

)



