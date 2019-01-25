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

# Convert an integer into full bitstring
get_bin = lambda x, n: format(x, 'b').zfill(n)


def decode_control_word(n0, n1):
    '''
    Read the control word used in ZLE data (see CAEN V1724 manual)
    '''
    # Convert the two 16-bit ints into one 32-bit string
    # Need to flip the order because it is converted as little-endian
    bits = get_bin(n1, 16) + get_bin(n0, 16)
    # Check if this is a control word for good or ZLE data
    is_zle = False
    if bits[0] == '0':
        is_zle = True
    n_samples = int(bits[11:], 2) * 2  # not sure where the factor of 2 came from
    return is_zle, n_samples


def split_data(doc, zle=True):
    '''
    Read data from doc and split it into list of ZLE chunks (i.e. pulses) and their offset
    If ZLE is off, this will just pass a list with one element (i.e. one big pulse) and a list with offset zero.
    '''
    # First decompress
    d = snappy.decompress(doc['data'])
    # Then read bytestring
    d = np.fromstring(d, dtype='<i2')
    if not zle:
        # There is just one giant pulse here
        return [d], [0]

    sample_index = 0
    i = 2  # Skip the first control word for unknown reason
    pulses = []
    pulses_start_samples = []
    while i < len(d):
        is_zle, n_samples = decode_control_word(*d[i:i + 2])
        if is_zle:
            sample_index += n_samples
        else:
            # TODO WARNING minus sign hardcoded
            pulses.append(-d[i + 2:i + 2 + n_samples])
            pulses_start_samples.append(sample_index)
            sample_index += n_samples
            i += n_samples
        i += 2
    return pulses, pulses_start_samples


@export
#@numba.jit(nopython=True, nogil=True, cache=True)
def mongo_to_records(collection_name,
                     samples_per_record=strax.DEFAULT_RECORD_LENGTH,
                     events_per_chunk=10):
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
    cursor = client[dbname][collection_name].find()

    for doc in cursor:
        pulses, pulses_start_samples = split_data(doc, zle=True)  # todo remove hard coding
        pulse_lengths = np.array([len(pulse) for pulse in pulses])

        n_records_tot = strax.records_needed(pulse_lengths,
                                             samples_per_record).sum()
        records = np.zeros(n_records_tot,
                           dtype=strax.record_dtype(samples_per_record))
        output_record_index = 0  # Record offset in data

        for p, p_start in zip(pulses, pulses_start_samples):
            n_records = strax.records_needed(len(p), samples_per_record)

            for rec_i in range(n_records):
                r = records[output_record_index]
                r['time'] = (doc['time']
                             + p_start * 10
                             + rec_i * samples_per_record * 10)
                r['channel'] = doc['channel'] + int(doc['module'] == 1724) * 8  # add 8 channels for V1724
                r['pulse_length'] = len(p)
                r['record_i'] = rec_i
                r['dt'] = 10

                # How much are we storing in this record?
                if rec_i != n_records - 1:
                    # There's more chunks coming, so we store a full chunk
                    n_store = samples_per_record
                    assert len(p) > samples_per_record * (rec_i + 1)
                else:
                    # Just enough to store the rest of the data
                    # Note it's not p.length % samples_per_record!!!
                    # (that would be zero if we have to store a full record)
                    n_store = len(p) - samples_per_record * rec_i

                assert 0 <= n_store <= samples_per_record
                r['length'] = n_store

                offset = rec_i * samples_per_record
                r['data'][:n_store] = p[offset:offset + n_store]
                output_record_index += 1

        results.append(records)
        if len(results) >= events_per_chunk:
            yield finish_results()


@export
@strax.takes_config(
    strax.Option('collection_name', default='', track=False,
                 help="Collection used, example: '190124_110558'"),
    strax.Option('events_per_chunk', default=50, track=False,
                 help="Number of events to yield per chunk"),
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
                events_per_chunk=self.config['events_per_chunk']

)



