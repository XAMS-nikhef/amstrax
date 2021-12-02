import os

import strax
import amstrax as ax
from immutabledict import immutabledict

common_opts = dict(
    register_all=[ax.pulse_processing,
                  ax.peak_processing,
                  ax.event_processing],
    register=[ax.DAQReader],
    store_run_fields=(
        'name', 'number',
        'start', 'end', 'livetime',
        'tags'),
    check_available=('raw_records_v1730',
                     'raw_records_v1724',
                     'records',
                     ))

common_config = dict(
    n_tpc_pmts=16,
    channel_map=immutabledict(
        v1730=(0, 8),  # related to raw_records_v1730
        v1724=(8, 16),  # related to raw_records_v1724
        aqmon=(40, 41)  # register strax deadtime
    ))


def xams(*args, **kwargs):
    if '_detector' in kwargs:
        raise ValueError('Don\'t specifify _detector!')
    mongo_kwargs = dict(mongo_collname='runs_gas',
                        runid_field='number',
                        mongo_dbname='run',
                        )
    return _xams_xamsl_context(*args, **kwargs, _detector='xamsl', mongo_kwargs=mongo_kwargs)


def xamsl(*args, **kwargs):
    if '_detector' in kwargs:
        raise ValueError('Don\'t specifify _detector!')
    mongo_kwargs = dict(mongo_collname='runs_gas',
                        runid_field='number',
                        mongo_dbname='run',
                        )
    return _xams_xamsl_context(*args, **kwargs, _detector='xamsl', mongo_kwargs=mongo_kwargs)


def _xams_xamsl_context(
        output_folder='./strax_data',
        raw_data_folder = '/data/xenon/{detector}/raw/',
        processed_data_folder = '/data/xenon/{detector}/processed/',
        _detector ='xams',
        init_rundb=True,
        mongo_kwargs: dict = None
        ):
    st = strax.Context(**common_opts,
                       **common_config,
                       forbid_creation_of=ax.DAQReader.provides,
                       )
    raw_data_folder=raw_data_folder.format(detector=_detector)
    processed_data_folder=processed_data_folder.format(detector=_detector)

    for p in [raw_data_folder, processed_data_folder]:
        if not os.path.exists(p):
            UserWarning(f'Context for {_detector}, folder {p} does not exist?!')

    if init_rundb:
        if mongo_kwargs is None:
            raise RuntimeError('You need to provide mongo-kwargs!')
        ax.RunDB(
            **mongo_kwargs
        ),
    st.storage = [
        strax.DataDirectory(raw_data_folder,
                            provide_run_metadata=False,
                            take_only=ax.DAQReader.provides,
                            deep_scan=False,
                            readonly=True),
        strax.DataDirectory(processed_data_folder,
                            provide_run_metadata=False,
                            deep_scan=False,
                            readonly=True),
        strax.DataDirectory(output_folder),
            ]
    return st


def amstrax_gas_test_analysis():
    """Return strax test for analysis of Xams gas test data"""
    UserWarning("Unsure if this context is complete and/or working")
    return strax.Context(
        storage=[
            ax.RunDB(
                mongo_url=f'mongodb://{os.environ["user"]}:{os.environ["password"]}@127.0.0.1:27017/admin',
                mongo_collname='runs_gas',
                runid_field='number',
                mongo_dbname='run'),
            strax.DataDirectory('/data/xenon/xams/strax_processed_gas/',
                                provide_run_metadata=False,
                                deep_scan=False,
                                readonly=True),
            strax.DataDirectory('/data/xenon/xams/strax_processed_peaks/',
                                provide_run_metadata=False,
                                deep_scan=False,
                                readonly=False,
                                )],
        forbid_creation_of='raw_records',
        **common_opts,
    )


def amstrax_gas_test_analysis_alt_baseline():
    """Return strax test for analysis of Xams gas test data"""
    UserWarning("Unsure if this context is complete and/or working")
    return strax.Context(
        storage=[
            strax.DataDirectory('/data/xenon/xams/strax_processed_gas/',
                                provide_run_metadata=False,
                                deep_scan=False,
                                readonly=False),
            strax.DataDirectory('/data/xenon/xams/strax_processed_peaks/',
                                provide_run_metadata=False,
                                deep_scan=False,
                                readonly=True,
                                )],
        forbid_creation_of='raw_records',
        register_all=[ax.daqreader, ax.pulse_processing_alt_baseline],
        store_run_fields=(
            'name', 'number',
            'start', 'end', 'livetime',
            'tags'),
    )


def amstrax_run10_analysis(output_folder='./strax_data'):
    """Return strax test for analysis of Xams gas test data"""
    UserWarning("Unsure if this context is complete and/or working")
    return strax.Context(
        storage=[
            strax.DataDirectory(f'{output_folder}',
                                provide_run_metadata=False,
                                deep_scan=False,
                                readonly=False),
        ],
        config=dict(**common_config),
        register=ax.RecordsFromPax,
        **common_opts
    )
