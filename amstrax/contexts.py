import os
from datetime import timezone

import strax
from immutabledict import immutabledict


import sys
import amstrax as ax

# Configuration
CONFIG = {"DEFAULT_DETECTOR": "xams", "DEFAULT_RUNCOLNAME": "run", "DEFAULT_COLLECTION": "runs_gas"}

PATHS_TO_REGISTER = [
    # The path for raw records on nikhef
    "/data/xenon/xams_v2/xams_raw_records",
    # The path for processed data on nikhef
    "/data/xenon/xams_v2/xams_processed",
    # The path for raw records on the xams server
    "/home/xams/data/xams_processed",
]


COMMON_OPT_XAMS = dict(
    register_all=[],
    register=[
        ax.DAQReader,
        ax.PulseProcessing,
        # Peaks
        ax.Peaks,
        ax.PeakBasics,
        ax.PeakPositions,
        # Events
        ax.Events,
        ax.EventBasics,
        ax.EventPositions,
        ax.EventCoincidences,
        ax.CorrectedAreas,
        ax.EventInfo,
        ax.EventWaveform,
        ax.EventAreaPerChannel,
        # External PMT plugins
        ax.PulseProcessingEXT,
        ax.PeaksEXT,
        ax.PeakBasicsEXT,
        # SiPMT plugins
        ax.PulseProcessingSiPM,
        ax.PeaksSiPM,
        ax.PeakBasicsSiPM,
        # Coincidences
        ax.PeakCoincidences,
        # LED plugins not default
        # ax.RecordsLED,
        # ax.LEDCalibration,
    ],
    store_run_fields=("name", "number", "start", "end", "livetime", "processing_status", "tags"),
    check_available=(),
    free_options=("live_data_dir",),
)

XAMS_COMMON_CONFIG = dict(
    n_tpc_pmts=5,
    channel_map=immutabledict(
        bottom=(0, 0),
        top=(1, 4),
        external=(5,5),
        sipm=(6, 6),
        aqmon=(40, 40),  # register strax deadtime
    ),
)


def xams(
    output_folder="./strax_data",
    init_rundb=True,
    mongo_kwargs=dict(
        mongo_collname=CONFIG["DEFAULT_COLLECTION"], mongo_dbname=CONFIG["DEFAULT_RUNCOLNAME"], runid_field="number"
    ),
    corrections_version=None,
    *args,
    **kwargs,
):

    st = strax.Context(
        **COMMON_OPT_XAMS, 
        forbid_creation_of=ax.DAQReader.provides,
        # output_folder it will be set again later, but so we do not create a folder /strax_data everywhere
        storage=output_folder, 
        )

    st.set_config(XAMS_COMMON_CONFIG)

    st.storage = []
    if init_rundb:
        if mongo_kwargs is None:
            raise RuntimeError("You need to provide mongo-kwargs!")
        st.storage = [
            ax.RunDB(
                **mongo_kwargs,
                provide_run_metadata=True,
            )
        ]

    for path in PATHS_TO_REGISTER:
        if os.path.exists(path):
            st.storage += [strax.DataDirectory(path, provide_run_metadata=False, deep_scan=False, readonly=True)]
        else:
            # just means we are in another ho
            pass

    st.storage += [
        strax.DataDirectory(output_folder),
    ]

    if corrections_version is not None:
        apply_global_correction_version(st, corrections_version)

    return st


def context_for_daq_reader(
    st: strax.Context,
    run_id: str,
    detector: str = "xams",
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
        raise ValueError(f"raw data is stored for {run_id} disable check by " f'setting "check_exists=False"')
    if runs_col_kwargs is None:
        runs_col_kwargs = {}
    if run_doc is None:
        run_col = ax.get_mongo_collection(detector)
        run_doc = run_col.find_one({"number": int(run_id)})
    daq_config = run_doc["daq_config"]

    live_dir = daq_config["strax_output_path"]

    # Check if live dir is set in the config, in case it is, set it
    # to the live dir in the config
    if "live_data_dir" in st.config:
        live_dir = st.config["live_data_dir"]
        # Print a UserWarning that the live_data_dir is overwritten
        UserWarning(f"live_data_dir is overwritten to {live_dir}")

    input_dir = os.path.join(live_dir, run_id)
    if not os.path.exists(input_dir):
        raise FileNotFoundError(f"No path at {input_dir}")

    
    def extract_channel_polarity(registers):
        channel_polarity = dict()
        for reg in registers:
            reg_addr = reg['reg'].lower()
            reg_val = reg['val'].lower()

            if reg_addr.startswith('1') and reg_addr.endswith('80'):
                try:
                    chan_num = (int(reg_addr, 16) - 0x1080) // 0x100

                    if (reg_val == "110000") or (reg_val == "1110000"):
                        channel_polarity[chan_num] = -1
                    elif (reg_val == "100000") or (reg_val == "1100000"):
                        channel_polarity[chan_num] = 1
                    else:
                        raise ValueError(f"Unknown polarity config value '{reg_val}' for channel {chan_num}")

                except Exception as e:
                    print(f"Error parsing register {reg_addr} with value {reg_val}: {e}")
        return channel_polarity

    # Extract the mapping
    channel_polarity_map = extract_channel_polarity(daq_config['registers'])

    st.set_context_config(dict(forbid_creation_of=tuple()))
    st.set_config(
        {'readout_threads': daq_config['processing_threads'],
         'daq_input_dir': input_dir,
         'record_length': daq_config['strax_fragment_payload_bytes'] // 2,
         'max_digitizer_sampling_time': 10,
         'run_start_time': run_doc['start'].replace(tzinfo=timezone.utc).timestamp(),
         'daq_chunk_duration': int(daq_config['strax_chunk_length'] * 1e9),
         'daq_overlap_chunk_duration': int(daq_config['strax_chunk_overlap'] * 1e9),
         'compressor': daq_config.get('compressor', 'lz4'),
         'channels_polarity': channel_polarity_map,
         })
    UserWarning(f'You changed the context for {run_id}. Do not process any other run!')
    return st


def xams_led(**kwargs):
    st = xams(**kwargs)
    st.set_context_config({"check_available": ("raw_records", "records_led", "led_calibration")})
    # Return a new context with only raw_records and led_calibration registered
    st = st.new_context(replace=True, config=st.config, storage=st.storage, **st.context_config)
    st.register([ax.DAQReader, ax.RecordsLED, ax.LEDCalibration])
    return st


def _check_raw_records_exists(st: strax.Context, run_id: str) -> bool:
    for plugin_name in st._plugin_class_registry.keys():
        if "raw" in plugin_name:
            if st.is_stored(run_id, plugin_name):
                return True
    return False


def apply_global_correction_version(context: strax.Context, global_version: str) -> None:
    """
    Set all the relevant correction variables based on the specified global version.
    Only for testing purposes, you can add a github branch to the version by adding '@branchname' to the version.

    :param global_version: A specific version (e.g., 'v0') to apply corrections.


    """
    # Load the global corrections file (e.g., 'global_v0.json')

    if "@" in global_version:
        global_version, github_branch = global_version.split("@")
    else:
        github_branch = "master"

    global_corrections_file = f"_global_{global_version}.json"
    global_corrections = ax.get_correction(global_corrections_file, branch=github_branch)

    # Iterate over all the relevant corrections specified in the global file
    xams_config = {}
    for correction_key, correction_value in global_corrections.items():
        if correction_value is None:
            warnings.warn(f"No correction file for {correction_key} in version {global_version}")
            continue

        # we need to check first if it's a string and if it ends with .json
        if isinstance(correction_value, str) and correction_value.endswith(".json"):
            # If the correction is a file, set the configuration to point to the correct file
            # Set the configuration to point to the correct file for each key (e.g., elife, gain)
            add_github_branch = "&github_branch=" + github_branch if github_branch is not None else ""
            config_value = f"file://{correction_key}?filename={correction_value}{add_github_branch}"
            xams_config[correction_key] = config_value

        else:
            # If the correction is a value, just set the value
            xams_config[correction_key] = correction_value

    # Set the full configuration in the context
    context.set_config(xams_config)

    # Add some logging or print statements if needed
    print(f"Applied XAMS version {global_version} with corrections: {xams_config}")

    return context
