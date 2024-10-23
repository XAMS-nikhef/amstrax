# processing.py
import os
import sys
import logging
import argparse
import strax
import socket
import getpass
import time, datetime

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


class RunProcessor:
    def __init__(self, args):
        self.run_id = f"{int(args.run_id):06d}"
        self.targets = args.targets
        self.output_folder = args.output_folder
        self.allow_raw_records = args.allow_raw_records
        self.corrections_version = args.corrections_version
        self.production = args.production
        self.amstrax_path = args.amstrax_path
        self.is_online = args.is_online
        
        if self.amstrax_path:
            self.amstrax_path = self.amstrax_path.rstrip("/")

        log.info(f"Processing run {self.run_id} with the following parameters:")
        log.info(f" --Targets: {self.targets}")
        log.info(f" --Output folder: {self.output_folder}")
        log.info(f" --Allow raw_records: {self.allow_raw_records}")
        log.info(f" --Corrections version: {self.corrections_version}")
        log.info(f" --Production: {self.production}")
        log.info(f" --Amstrax path: {self.amstrax_path}")
        log.info(f" --This file: {__file__}")
        log.info(f" --Is online: {self.is_online}")

        self.setup_amstrax()
        self.setup_production()
        self.run_doc = self.db_utils.get_run_doc(self.run_id)
        self.infer_special_modes()


    def setup_amstrax(self):

        if self.amstrax_path:
            if not os.path.exists(self.amstrax_path):
                raise FileNotFoundError(f"amstrax path {self.amstrax_path} does not exist.")
            log.info(f"Adding {self.amstrax_path} to sys.path.")
            sys.path.insert(0, self.amstrax_path)
        import amstrax
        self.amstrax = amstrax
        log.info(f"Using amstrax version: {amstrax.__version__} at {amstrax.__file__}")

        self.db_utils = self.amstrax.db_utils

    def get_run_doc_info(self):

        # let's just print out the main info from the rundoc
        # like mode, start, end, duration, user, comments, tags
        # we'll need to format the date and time properly, and calculate the duration
        # use datetime to convert the date to a readable format
        log.info(f"Run document for run {self.run_id}:")
        log.info(f" *** Mode: {self.run_doc.get('mode')}")
        log.info(f" *** Start: {datetime.datetime.fromtimestamp(self.run_doc.get('start') / 1e9)}")
        log.info(f" *** End: {datetime.datetime.fromtimestamp(self.run_doc.get('end') / 1e9)}")
        log.info(f" *** Duration: {(self.run_doc.get('end') - self.run_doc.get('start'))/1e9:.2f} seconds")
        log.info(f" *** User: {self.run_doc.get('user')}")
        log.info(f" *** Comments: {self.run_doc.get('comments')}")
        log.info(f" *** Tags: {self.run_doc.get('tags')}")

    def infer_special_modes(self):

        # Check if there is led in the run_doc
        if "ledcalibration" in self.run_doc.get('mode'):
            # Add the LEDCalibration plugin to the context
            log.info("Detected LED calibration run.")
            log.info("Adding LEDCalibration plugin to the context.")
            ax = self.amstrax
            self.st.register([
                ax.DAQReader,
                ax.RecordsLED,
                ax.LEDCalibration
            ])
        
            # override the targets to process only the LEDCalibration
            self.targets = ["raw_records", "records_led", "led_calibration"]
        

    def setup_production(self):

        if self.production:
            log.info("Setting up production configurations.")

            # Set the output folder to the production folder
            if self.output_folder:
                raise ValueError("Output folder should not be set when processing production data.")

            self.output_folder = self.amstrax.get_xams_config("xams_processed_folder")
            log.info(f"Output folder set to {self.output_folder}")

            # Make sure we specified corrections version
            if not self.corrections_version:
                raise ValueError("Corrections version should be specified for production processing.")

            # make sure that amstrax_path contains amstrax_versioned
            if "amstrax_versioned" not in self.amstrax_path:
                raise ValueError("amstrax_path should be from amstrax_versioned for production processing.")

            # only xamsdata user can process production data
            if getpass.getuser() != "xamsdata":
                raise PermissionError("Only xamsdata user can process production data.")


    def add_data_entry(self, data_type, location, **info):
        """
        Add the data entry to rundb.
        """
        self.db_utils.add_data_entry(
            run_id=self.run_id,
            production=self.production,
            host=socket.gethostname().split("-")[0],
            location=location,
            user=getpass.getuser(),
            corrections_version=self.corrections_version,
            amstrax_path=self.amstrax_path,
            amstrax_version=self.amstrax.__version__,
            updated_at=datetime.datetime.now(),
            is_online=self.is_online,
            data_type=data_type,
            **info
        )


    def process(self):
        # Split targets into raw_records and others
        if "raw_records" in self.targets:
            if not self.allow_raw_records:
                log.error("Raw records processing is not allowed. Use the --allow_raw_records flag.")
                return
            self.targets.remove("raw_records")
            self.process_raw_records()

        if self.targets:
            self.process_remaining_targets()

    def get_info_from_processed_data(self, folder, target, st):
        # Logic for getting info from processed data (similar to the existing one)
        # When the processing succeeds, the data is stored in the output folder
        # and the data entry is added to the database.
        # We want to know how many files are in the created folder
        # what is the lineage_hash 
        # and the total size of the data in MB

        key_for = str(st.key_for(self.run_id, target))
        log.info(f"Getting info from processed data in {folder} for {key_for}")
        data_folder = os.path.join(folder, key_for)
        lineage_hash = key_for.split("-")[-1]

        size_mb = 0
        n_files = 0

        for root, dirs, files in os.walk(data_folder):
            for file in files:
                size_mb += os.path.getsize(os.path.join(root, file)) / 1e6
                n_files += 1

        log.info(f"Processed data in {data_folder} contains {n_files} files with a total size of {size_mb:.2f} MB.")

        res = {
            "n_chunks": n_files,
            "lineage_hash": lineage_hash,
            "size_mb": size_mb
        }

        return res
        
    def process_raw_records(self):
        raw_records_folder = self.amstrax.get_xams_config("raw_records_folder")
        live_folder = self.amstrax.get_xams_config("live_folder")

        rundb = self.amstrax.get_mongo_collection()
        run_doc = rundb.find_one({"number": int(self.run_id)})
        if not run_doc:
            log.error(f"Run document for {self.run_id} not found in rundb.")
            return

        log.info("Processing raw_records separately...")
        log.info(f"Live data directory for run {self.run_id}: {live_folder}")

        raw_st = self.amstrax.contexts.xams(
            output_folder=raw_records_folder,
            init_rundb=False,
            corrections_version=self.corrections_version
        )
        raw_st.storage += [strax.DataDirectory(live_folder, readonly=True)]
        raw_st.set_config({"live_data_dir": live_folder})

        self.raw_st = raw_st
        target = "raw_records"

        if self.raw_st.is_stored(self.run_id, target):
            log.info(f"Skipping {target} for run {self.run_id} as it is already processed.")
            return

        try:
            log.info(f"Processing raw_records for run {self.run_id}")
            self.raw_st.make(self.run_id, target, progress_bar=True)
            self.db_utils.update_processing_status(self.run_id, "done", production=self.production, is_online=self.is_online)
            info = self.get_info_from_processed_data(raw_records_folder, target, self.raw_st)
            self.add_data_entry(data_type=target, location=raw_records_folder, **info)

        except Exception as e:
            log.error(f"Failed to process raw_records for run {self.run_id}: {e}")
            self.db_utils.update_processing_status(self.run_id, "failed", reason=str(e), production=self.production, is_online=self.is_online)


    def process_remaining_targets(self):
        raw_records_folder = self.amstrax.get_xams_config("raw_records_folder")

        log.info(f"Processing remaining targets: {self.targets}")
        log.info(f"Output folder: {self.output_folder}")

        st = self.amstrax.contexts.xams(
            output_folder=self.output_folder,
            corrections_version=self.corrections_version
        )
        st.storage += [strax.DataDirectory(raw_records_folder, readonly=True)]

        self.st = st

        try:
            for target in self.targets:
                if self.st.is_stored(self.run_id, target):
                    log.info(f"Skipping {target} for run {self.run_id} as it is already processed.")
                    continue

                t = time.time()
                log.info(f"Processing {target} for run {self.run_id}")
                self.st.make(self.run_id, target, progress_bar=True)
                log.info(f"Processing of {target} completed successfully ({time.time() - t:.2f}s).")
                info = self.get_info_from_processed_data(self.output_folder, target, self.st)
                self.add_data_entry(data_type=target, location=self.output_folder, **info)

            self.db_utils.update_processing_status(self.run_id, "done", production=self.production, is_online=self.is_online)
            log.info(f"Processing of run {self.run_id} completed successfully.")

        except Exception as e:
            log.error(f"Processing of targets failed for run {self.run_id}: {e}")
            self.db_utils.update_processing_status(self.run_id, "failed", reason=str(e), production=self.production, is_online=self.is_online)


def parse_args():
    parser = argparse.ArgumentParser(description="Process a single run using amstrax.")
    parser.add_argument("--run_id", type=str, help="Run ID to process.")
    parser.add_argument("--targets", nargs="+", help="List of data types to process (e.g., 'raw_records', 'peaks').")
    parser.add_argument("--output_folder", type=str, help="Path to save the processed data.", default=None)
    parser.add_argument("--allow_raw_records", action="store_true", help="Explicitly allow raw_records processing.")
    parser.add_argument("--corrections_version", type=str, default=None, help="Version of corrections to apply.")
    parser.add_argument("--amstrax_path", type=str, default=None, help="Version of amstrax to use.")
    parser.add_argument("--production", action="store_true", help="Update the production database.")
    parser.add_argument("--is_online", action="store_true", help="Process online data.")
    return parser.parse_args()


def main():
    args = parse_args()
    processor = RunProcessor(args)
    processor.process()


if __name__ == "__main__":
    main()
