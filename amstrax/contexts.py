import strax
import amstrax as ax


common_opts = dict(
    register= [ax.daqreader.DAQReader],
    register_all=[ax.pulse_processing,
                  ax.peak_processing,
                  ax.event_processing],
    store_run_fields=(
        'name', 'number',
        'start', 'end', 'livetime',
        'tags'),
    check_available=('raw_records', 'records',
                     ))

def amstrax_gas_test_analysis():
    """Return strax test for analysis of Xams gas test data"""
    return strax.Context(
        storage = [
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
        storage = [
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