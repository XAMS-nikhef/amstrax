import strax
import amstrax as ax
from immutabledict import immutabledict

common_opts = dict(
    register_all=[ax.pulse_processing,
                  ax.peak_processing,
                  ax.event_processing],
    register=[ax.daqreader.DAQReader],
    store_run_fields=(
        'name', 'number',
        'start', 'end', 'livetime',
        'tags'),
    check_available=('raw_records', 'records',
                     ))

common_opts_xamsl = dict(
    register_all=[ax.pulse_processing,
                  ax.peak_processing, ],
    register=[ax.daqreader.DAQReaderXamsl],
    store_run_fields=(
        'name', 'number',
        'start', 'end', 'livetime',
        'tags'),
    check_available=('raw_records_xamsl', 'records',))

common_config = dict(
    n_tpc_pmts=16,
    channel_map=immutabledict(
        sipm=(0, 8),  # related to raw_records_v1730
        pmt=(8, 16),  # related to raw_records_v1724
    ))

common_config_xamsl = dict(
    n_tpc_pmts=8,  # total number of available channels
    channel_map=immutabledict(pmt=(0, 8)), )


def amstrax_gas_test_analysis():
    """Return strax test for analysis of Xams gas test data"""
    return strax.Context(
        storage=[
            ax.RunDB(
                mongo_url='mongodb://user:password@127.0.0.1:27017/admin',
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
    return strax.Context(
        storage=[
            # ax.RunDB(
            #     mongo_url='mongodb://user:password@127.0.0.1:27017/admin',
            #     mongo_collname='runs_gas',
            #     runid_field='number',
            #     mongo_dbname='run'),
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
