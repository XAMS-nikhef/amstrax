import amstrax_files
import strax
import typing as ty
from strax import Config
from immutabledict import immutabledict
from urllib.parse import urlparse, parse_qs

export, __all__ = strax.exporter()


@export
class XAMSConfig(Config):
    """A configuration class that fetches corrections from JSON files."""

    def __init__(self, name="", version="v0", **kwargs):
        super().__init__(name=name, **kwargs)
        self.version = version  # Track the version
        self.correction_hash = None  # Store correction hash for lineage

    def fetch(self, plugin):
        """
        Overrides the fetch method to load corrections from JSON or CMT files.
        Handles both 'cmt://' and 'file://' URLs. Otherwise, returns default.
        """

        config_value = plugin.config.get(self.name, self.default)

        # Check if the config is a cmt URL or file URL
        if isinstance(config_value, str):
            if config_value.startswith("cmt://"):
                return self._fetch_from_cmt(plugin, config_value)
            elif config_value.startswith("file://"):
                return self._fetch_from_file_url(plugin, config_value)
        
        # Return regular default value if no URL
        return plugin.config.get(self.name, self.default)

    def _fetch_from_cmt(self, plugin, config_value):
        """Fetch correction from a cmt:// URL."""
        run_id = plugin.run_id
        correction_key = self.name

        print(f"Fetching correction for {correction_key} and run_id {run_id}")

        # Retrieve the global corrections file
        corrections = amstrax_files.get_correction(f"_global_{self.version}.json")

        # Get the specific file for this correction (e.g., 'elife_v0.json')
        correction_file = corrections.get(correction_key)
        if not correction_file:
            raise ValueError(f"No correction file found for {correction_key} and run_id {run_id}")

        # Load the correction data (e.g., {'001200': 5500, '001300': 6000})
        correction_data = amstrax_files.get_correction(correction_file)

        value = self.find_correction_value(correction_data, run_id)

        # Store the correction hash to ensure lineage tracking
        self.correction_hash = self.get_correction_hash(correction_file)

        return value

    def _fetch_from_file_url(self, plugin, config_value):
        """Fetch correction from a file:// URL."""
        # Parse the URL to extract filename and run_id
        parsed_url = urlparse(config_value)
        query_params = parse_qs(parsed_url.query)

        filename = query_params.get("filename", [None])[0]
        run_id = query_params.get("run_id", [None])[0] or plugin.run_id

        if not filename:
            raise ValueError(f"Invalid file:// URL, missing filename: {config_value}")

        # Retrieve the specific correction file (e.g., 'elife_v0.json')
        correction_data = amstrax_files.get_correction(filename)

        # Find the correction value based on the run_id
        value = self.find_correction_value(correction_data, run_id)

        return value

def find_correction_value(self, correction_data, run_id):
    run_id = run_id.zfill(6)  # Ensure run_id is always 6 digits
    value = None

    for run_range in correction_data.keys():
        if "-" in run_range:
            start_run, end_run = run_range.split("-")
            start_run = start_run.zfill(6)
            if end_run == "*":
                # Only allow * for online corrections
                # check if there is _dev in the filename
                if "_dev" not in self.name:
                    raise ValueError(f"Wildcard '*' is only allowed for online corrections")
                end_run = "999999"  # Treat * as the highest possible run ID
            end_run = end_run.zfill(6)

            if start_run <= run_id <= end_run:
                value = correction_data[run_range]
                break
        else:
            if run_range.zfill(6) == run_id:
                value = correction_data[run_range]
                break

    if value is None:
        raise ValueError(f"No valid correction found for run_id {run_id} and no fallback is allowed.")

    return value

