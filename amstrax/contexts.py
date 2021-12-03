import os
from datetime import timezone
import strax
import amstrax as ax
from immutabledict import immutabledict

common_opts = dict(
    # register_all=[ax.pulse_processing,
    #               ax.peak_processing,
    #               ax.event_processing],
    register=[ax.DAQReader],
    store_run_fields=(
        'name', 'number',
        'start', 'end', 'livetime',
        'tags'),
    check_available=('raw_records_v1730',
                     'raw_records_v1724',
                     'records',
                     ),
    free_options=('live_data_dir',),
)

# xamsl and xams are too similar
xams_little_common_config = dict(
    live_data_dir='/data/xenon/xamsl/live_data',
    n_tpc_pmts=4,
    channel_map=immutabledict(
        v1730=(0, 2),
        v1724=(2, 4),
        aqmon=(40, 41),  # register strax deadtime
    ))

xams_common_config = dict(
    live_data_dir='/data/xenon/xams/live_data',  # doesn't work yet
    n_tpc_pmts=16,
    channel_map=immutabledict(
        v1730=(0, 8),
        v1724=(8, 16),
        aqmon=(40, 41),  # register strax deadtime
    ))


def xams(*args, **kwargs):
    if '_detector' in kwargs:
        raise ValueError('Don\'t specifify _detector!')
    mongo_kwargs = dict(mongo_collname='runs_gas',
                        runid_field='number',
                        mongo_dbname='run',
                        )
    st = _xams_xamsl_context(*args, **kwargs, _detector='xamsl', mongo_kwargs=mongo_kwargs)
    st.set_config(xams_common_config)
    return st


def xams_little(*args, **kwargs):
    if '_detector' in kwargs:
        raise ValueError('Don\'t specifify _detector!')
    mongo_kwargs = dict(mongo_collname='runs_new',
                        runid_field='number',
                        mongo_dbname='run',
                        )

    st = _xams_xamsl_context(*args, **kwargs, _detector='xamsl', mongo_kwargs=mongo_kwargs)
    st.set_config(xams_little_common_config)
    return st


def _xams_xamsl_context(
        output_folder='./amstrax_data',
        raw_data_folder='/data/xenon/{detector}/raw/',
        processed_data_folder='/data/xenon/{detector}/processed/',
        _detector='xams',
        init_rundb=True,
        mongo_kwargs: dict = None
):
    st = strax.Context(**common_opts,
                       forbid_creation_of=ax.DAQReader.provides,
                       )
    raw_data_folder = raw_data_folder.format(detector=_detector)
    processed_data_folder = processed_data_folder.format(detector=_detector)

    for p in [raw_data_folder, processed_data_folder]:
        if not os.path.exists(p):
            UserWarning(f'Context for {_detector}, folder {p} does not exist?!')

    st.storage = []
    if init_rundb:
        if mongo_kwargs is None:
            raise RuntimeError('You need to provide mongo-kwargs!')
        st.storage = [ax.RunDB(
            **mongo_kwargs,
            provide_run_metadata=True,
        )]
    st.storage += [
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
    print(st.storage)
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
        config=dict(**xams_common_config),
        register=ax.RecordsFromPax,
        **common_opts
    )


def context_for_daq_reader(st: strax.Context,
                           run_id: str,
                           runs_col_kwargs: dict = None,
                           run_doc: dict = None,
                           check_exists=True,
                           ):
    """
    Given a context and run_id, change the options such that we can
    process the live data.

    IMPORTANT:
    After setting the context, we specify the location of the live-data
    for a single run. This means you CANNOT re-use this context!
    Therefore, if you want to process data, you should start a new
    context if you want to process another run starting from the live
    data

    :param st: Context to change
    :param run_id: the run_id of the run that should be processed
    :param runs_col_kwargs: Optional options (kwargs) for starting the
        run-collection, see `get_mongo_collection`
    :param run_doc: Optional document associated with this run-id.
    :return: Context ready to start processing <run_id> with from the
        live-data
    """
    if check_exists and _check_raw_records_exists(st, run_id):
        raise ValueError(f'raw data is stored for {run_id} disable check by '
                         f'setting "check_exists=False"')
    if runs_col_kwargs is None:
        runs_col_kwargs = {}
    if run_doc is None:
        run_col = ax.get_mongo_collection(**runs_col_kwargs)
        run_doc = run_col.find_one({'number': int(run_id)})
    daq_config = run_doc['daq_config']
    live_dir = st.config['live_data_dir']

    input_dir = os.path.join(live_dir, run_id)
    if not os.path.exists(input_dir):
        raise FileNotFoundError(f'No path at {input_dir}')

    st.set_context_config(dict(forbid_creation_of=tuple()))
    st.set_config(
        {'readout_threads': daq_config['processing_threads'],
         'daq_input_dir': input_dir,
         'record_length': daq_config['strax_fragment_payload_bytes'] // 2,
         'max_digitizer_sampling_time': 10,
         'run_start_time': run_doc['start'].replace(tzinfo=timezone.utc).timestamp(),
         'daq_chunk_duration': int(daq_config['strax_chunk_length'] * 1e9),
         'daq_overlap_chunk_duration': int(daq_config['strax_chunk_overlap'] * 1e9),
         'compressor': daq_config.get('compressor', 'lz4')
         })
    UserWarning(f'You changed the context for {run_id}. Do not process any other run!')
    return st


def _check_raw_records_exists(st: strax.Context, run_id: str) -> bool:
    for plugin_name in st._plugin_class_registry.keys():
        if 'raw' in plugin_name:
            if st.is_stored(run_id, plugin_name):
                return True
    return False
