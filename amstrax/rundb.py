import os
import re
import socket
import typing

import pymongo
import strax
from sshtunnel import SSHTunnelForwarder
from tqdm import tqdm

export, __all__ = strax.exporter()

default_mongo_dbname = 'run'
default_mongo_collname = 'runs'

_SECRET_SERVING_PORT = {}


def _check_environment_var(key):
    if key not in os.environ:
        raise RuntimeError(
            f"{key} not set. Please define in .bashrc file. (i.e. "
            f"'export {key} = <secret {key.lower()}>')")


def link_to_daq(
        daq_host="145.102.134.61",
        daq_user="xams"
):
    """Create an SSH tunnel to the daq machine to get access to the runsdb"""
    port_key = f'{daq_host}_{daq_user}'
    if _SECRET_SERVING_PORT is not None and port_key in _SECRET_SERVING_PORT:
        return _SECRET_SERVING_PORT[port_key]
    _check_environment_var("DAQ_PASSWORD")
    daq_password = os.environ['DAQ_PASSWORD']
    server = SSHTunnelForwarder(
        daq_host,
        ssh_username=daq_user,
        ssh_password=daq_password,
        remote_bind_address=('127.0.0.1', 27017),
    )
    server.start()
    _SECRET_SERVING_PORT[port_key] = server.local_bind_port
    return server.local_bind_port


@export
def get_mongo_client(**link_kwargs):
    """Get a mongo client, any kwargs are passed on to link_to_daq"""
    _check_environment_var('MONGO_USER')
    _check_environment_var('MONGO_PASSWORD')
    local_port = link_to_daq(**link_kwargs)
    user = os.environ['MONGO_USER']
    password = os.environ['MONGO_PASSWORD']
    return pymongo.MongoClient(f'mongodb://{user}:{password}@127.0.0.1:{local_port}/admin')


# @export
# def get_mongo_collection(database_name='run',
#                          database_col='runs_new',
#                          **link_kwargs,
#                          ):
#     """Get the runs collection"""
#     return get_mongo_client(**link_kwargs)[database_name][database_col]

@export
def get_mongo_collection(detector,
                         **link_kwargs,
                         ):
    if detector == 'xams':
        return get_mongo_client(**link_kwargs)['run']['runs_gas']
    elif detector == 'xamsl':
        return get_mongo_client(**link_kwargs)['run']['runs_new']
    else:
        raise NameError(f'detector {detector} is not a valid detector name.')

@export
class RunDB(strax.StorageFrontend):
    """Frontend that searches RunDB MongoDB for data.

    Loads appropriate backends ranging from Files to S3.
    """
    # Dict of alias used in rundb: regex on hostname
    hosts = {
        'dali': r'^dali.*rcc.*',
    }

    provide_run_metadata = True

    def __init__(self,
                 mongo_dbname=None,
                 mongo_collname=None,
                 runid_field='name',
                 local_only=True,
                 new_data_path=None,
                 reader_ini_name_is_mode=False,
                 readonly=True,
                 *args,
                 **kwargs):
        """
        :param mongo_url: URL to Mongo runs database (including auth)
        :param local_only: Do not show data as available if it would have to be
        downloaded from a remote location.
        :param new_data_path: Path where new files are to be written.
            Defaults to None: do not write new data
            New files will be registered in the runs db!
        :param runid_field: Rundb field to which strax's run_id concept
            corresponds. Can be either
            - 'name': values must be strings, for XENON1T
            - 'number': values must be ints, for XENONnT DAQ tests
        :param reader_ini_name_is_mode: If True, will overwrite the 'mode'
        field with 'reader.ini.name'.

        Other (kw)args are passed to StorageFrontend.__init__
        """
        super().__init__(*args, **kwargs)
        self.local_only = local_only
        self.new_data_path = new_data_path
        self.reader_ini_name_is_mode = reader_ini_name_is_mode
        self.readonly = readonly
        if self.new_data_path is None:
            self.readonly = True

        self.runid_field = runid_field

        if self.runid_field not in ['name', 'number']:
            raise ValueError("Unrecognized runid_field option %s" % self.runid_field)

        self.client = get_mongo_client()

        if mongo_dbname is None:
            mongo_dbname = default_mongo_dbname
        if mongo_collname is None:
            mongo_collname = default_mongo_collname
        self.collection = self.client[mongo_dbname][mongo_collname]

        self.backends = [
            strax.FileSytemBackend(),
        ]

        # Construct mongo query for runs with available data.
        # This depends on the machine you're running on.
        self.hostname = socket.getfqdn()
        self.available_query = [{'host': self.hostname}]

        # Go through known host aliases
        for host_alias, regex in self.hosts.items():
            if re.match(regex, self.hostname):
                self.available_query.append({'host': host_alias})

    def _data_query(self, key):
        """Return MongoDB query for data field matching key"""
        return {
            'data': {
                '$elemMatch': {
                    'type': key.data_type,
                    'meta.lineage': key.lineage,
                    '$or': self.available_query}}}

    def _find(self, key: strax.DataKey,
              write, allow_incomplete, fuzzy_for, fuzzy_for_options):
        if fuzzy_for or fuzzy_for_options:
            raise NotImplementedError("Can't do fuzzy with RunDB yet.")

        # Check if the run exists
        if self.runid_field == 'name':
            run_query = {'name': key.run_id}
        else:
            run_query = {'number': int(key.run_id)}
        dq = self._data_query(key)
        doc = self.collection.find_one({**run_query, **dq},
                                       projection=dq)
        if doc is None:
            # Data was not found
            if not write:
                raise strax.DataNotAvailable

            output_path = os.path.join(self.new_data_path, str(key))

            if self.new_data_path is not None:
                doc = self.collection.find_one(run_query, projection={'_id'})
                if not doc:
                    raise ValueError(
                        f"Attempt to register new data for non-existing run {key.run_id}")  # noqa
                self.collection.find_one_and_update(
                    {'_id': doc['_id']},
                    {'$push': {'data': {
                        'location': output_path,
                        'host': self.hostname,
                        'type': key.data_type,
                        'protocol': strax.FileSytemBackend.__name__,
                        'meta': {'lineage': key.lineage}
                    }}})

            return (strax.FileSytemBackend.__name__,
                    output_path)

        datum = doc['data'][0]

        if write and not self._can_overwrite(key):
            raise strax.DataExistsError(at=datum['location'])

        return datum['protocol'], datum['location']

    def find_several(self, keys: typing.List[strax.DataKey], **kwargs):
        if kwargs['fuzzy_for'] or kwargs['fuzzy_for_options']:
            raise NotImplementedError("Can't do fuzzy with RunDB yet.")
        if not len(keys):
            return []
        if not len(set([k.lineage_hash for k in keys])) == 1:
            raise ValueError("find_several keys must have same lineage")
        if not len(set([k.data_type for k in keys])) == 1:
            raise ValueError("find_several keys must have same data type")
        keys = list(keys)  # Context used to pass a set

        if self.runid_field == 'name':
            run_query = {'name': {'$in': [key.run_id for key in keys]}}
        else:
            run_query = {'name': {'$in': [int(key.run_id) for key in keys]}}
        dq = self._data_query(keys[0])

        projection = dq.copy()
        projection.update({
            k: True
            for k in 'name number data.protocol data.location'.split()})

        results_dict = dict()
        for doc in self.collection.find(
                {**run_query, **dq}, projection=projection):
            datum = doc['data'][0]

            if self.runid_field == 'name':
                dk = doc['name']
            else:
                dk = doc['number']
            results_dict[dk] = datum['protocol'], datum['location']

        return [
            results_dict.get(k.run_id, False)
            for k in keys]

    def _list_available(self,
                        key: strax.DataKey,
                        allow_incomplete,
                        fuzzy_for,
                        fuzzy_for_options):
        if fuzzy_for or fuzzy_for_options:
            raise NotImplementedError("Can't do fuzzy with RunDB yet.")

        dq = self._data_query(key)
        cursor = self.collection.find(
            dq,
            projection=[self.runid_field])
        return [x[self.runid_field] for x in cursor]

    def _scan_runs(self, store_fields):
        cursor = self.collection.find(
            filter={},
            projection=strax.to_str_tuple(
                list(store_fields) + ['reader.ini.name']))
        for doc in tqdm(cursor, desc='Fetching run info from MongoDB',
                        total=cursor.count()):
            del doc['_id']
            if self.reader_ini_name_is_mode:
                doc['mode'] = \
                    doc.get('reader', {}).get('ini', {}).get('name', '')
            yield doc

    def run_metadata(self, run_id, projection=None):
        doc = self.collection.find_one({'number': int(run_id)}, projection=projection)
        if self.reader_ini_name_is_mode:
            doc['mode'] = doc.get('reader', {}).get('ini', {}).get('name', '')
        if doc is None:
            raise strax.DataNotAvailable
        return doc
