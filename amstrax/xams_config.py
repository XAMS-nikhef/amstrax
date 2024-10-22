import strax
import typing as ty
from strax import Config
from immutabledict import immutabledict
from urllib.parse import urlparse, parse_qs
import amstrax

export, __all__ = strax.exporter()


@export
class XAMSConfig(Config):
    """A configuration class that fetches corrections from JSON files."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._cached_value = None  # Initialize cache for the fetched value

    def fetch(self, plugin):
        """
        Overrides the fetch method to load corrections from JSON or CMT files.
        Handles both 'cmt://' and 'file://' URLs. Otherwise, returns default.
        """

        # Check if the value has already been cached
        if self._cached_value is not None:
            print(f"Using cached value for {self.name}")
            return self._cached_value

        config_value = plugin.config.get(self.name, self.default)

        # Check if the config is a cmt URL or file URL
        if isinstance(config_value, str):
            if config_value.startswith("cmt://"):
                self._cached_value = self._fetch_from_cmt(plugin, config_value)
            elif config_value.startswith("file://"):
                self._cached_value = self._fetch_from_file_url(plugin, config_value)
        else:
            # Cache the default value if no URL is provided
            self._cached_value = config_value

        return self._cached_value

    def _fetch_from_cmt(self, plugin, config_value):
        """Fetch correction from a cmt:// URL."""
        correction_key = self.name

        parsed_url = urlparse(config_value)
        query_params = parse_qs(parsed_url.query)
        run_id = plugin.run_id
        github_branch = query_params.get("github_branch", ["master"])[0]
        version = query_params.get("version", [None])[0]
        if not version:
            raise ValueError(f"Invalid cmt:// URL, missing version: {config_value}")

        print(f"Fetching correction {version} for {correction_key} and run_id {run_id} using branch {github_branch}")

        # Retrieve the global corrections file
        corrections = amstrax.get_correction(f"_global_{version}.json", branch=github_branch)

        # Get the specific file for this correction (e.g., 'elife_v0.json')
        correction_file = corrections.get(correction_key)
        if not correction_file:
            raise ValueError(f"No correction file found for {correction_key} and run_id {run_id}")

        # Load the correction data (e.g., {'001200': 5500, '001300': 6000})
        correction_data = amstrax.get_correction(correction_file, branch=github_branch)

        value = self.find_correction_value(correction_data, run_id)

        return value

    def _fetch_from_file_url(self, plugin, config_value):
        """Fetch correction from a file:// URL."""
        # Parse the URL to extract filename and run_id
        parsed_url = urlparse(config_value)
        query_params = parse_qs(parsed_url.query)
        filename = query_params.get("filename", [None])[0]
        run_id = plugin.run_id

        github_branch = query_params.get("github_branch", ["master"])[0]

        self.filename = filename

        if not filename:
            raise ValueError(f"Invalid file:// URL, missing filename: {config_value}")

        # Retrieve the specific correction file (e.g., 'elife_v0.json')
        correction_data = amstrax.get_correction(filename, branch=github_branch)

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
                    if "_dev" not in self.filename:
                        raise ValueError(f"Wildcard '*' is only allowed for online corrections")
                    end_run = "999999"  # Treat * as the highest possible run ID

                if start_run == "*":
                    # Only allow * for online corrections
                    # check if there is _dev in the filename
                    if "_dev" not in self.filename:
                        raise ValueError(f"Wildcard '*' is only allowed for online corrections")
                    start_run = "000000"

                end_run = end_run.zfill(6)

                print(f"Checking if {start_run} <= {run_id} <= {end_run}")

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
