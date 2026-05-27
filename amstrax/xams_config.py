import strax
import typing as ty
from strax import Config
from urllib.parse import urlparse, parse_qs
from immutabledict import immutabledict
import amstrax

export, __all__ = strax.exporter()

DEFAULT_CHANNEL_MAP = immutabledict(
    bottom=(0, 0),
    top=(1, 4),
    external=(5, 5),
    sipm=(6, 6),
    aqmon=(40, 40),
)


def _as_immutabledict_channel_map(value):
    if isinstance(value, immutabledict):
        return value
    if not isinstance(value, dict):
        return value
    out = {}
    for k, v in value.items():
        if isinstance(v, (list, tuple)) and len(v) == 2:
            out[str(k)] = (int(v[0]), int(v[1]))
        else:
            out[str(k)] = v
    return immutabledict(out)


@export
class XAMSConfig(Config):
    """A configuration class that fetches corrections from JSON files."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._cache = {}

    def fetch(self, plugin):
        """
        Overrides the fetch method to load corrections from JSON or CMT files.
        Handles both 'cmt://' and 'file://' URLs. Otherwise, returns default.
        """
        config_value = plugin.config.get(self.name, self.default)
        run_id = getattr(plugin, "run_id", None)

        # Check if the config is a cmt URL or file URL
        if isinstance(config_value, str):
            cache_key = (config_value, str(run_id))
            if cache_key in self._cache:
                return self._cache[cache_key]

            if config_value.startswith("cmt://"):
                value = self._fetch_from_cmt(plugin, config_value)
            elif config_value.startswith("file://"):
                value = self._fetch_from_file_url(plugin, config_value)
            elif config_value.startswith("rundoc://"):
                value = self._fetch_from_rundoc_url(plugin, config_value)
            else:
                value = config_value

            self._cache[cache_key] = value
            return value

        return config_value

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

        value = self.find_correction_value(correction_data, run_id, correction_file=correction_file)

        return value

    def _fetch_from_file_url(self, plugin, config_value):
        """Fetch correction from a file:// URL."""
        # Parse the URL to extract filename and run_id
        parsed_url = urlparse(config_value)
        query_params = parse_qs(parsed_url.query)
        filename = query_params.get("filename", [None])[0]
        run_id = plugin.run_id

        github_branch = query_params.get("github_branch", ["master"])[0]

        if not filename:
            raise ValueError(f"Invalid file:// URL, missing filename: {config_value}")

        # Retrieve the specific correction file (e.g., 'elife_v0.json')
        correction_data = amstrax.get_correction(filename, branch=github_branch)

        # Find the correction value based on the run_id
        value = self.find_correction_value(correction_data, run_id, correction_file=filename)

        return value

    def _fetch_from_rundoc_url(self, plugin, config_value):
        """
        Fetch value from rundoc.
        URL format:
            rundoc://?path=xams_bookkeeping.channel_map
            rundoc://?path=xams_bookkeeping.source_type&detector=xams
        """
        parsed_url = urlparse(config_value)
        query_params = parse_qs(parsed_url.query)
        path = query_params.get("path", [None])[0]
        detector = query_params.get("detector", ["xams"])[0]
        fallback = query_params.get("fallback", [""])[0]
        if not path:
            raise ValueError(f"Invalid rundoc:// URL, missing path: {config_value}")
        value = get_rundoc_value(run_id=plugin.run_id, path=path, detector=detector, default=None)
        if value is None:
            if fallback == "xams_default":
                return DEFAULT_CHANNEL_MAP
            if fallback == "empty":
                return []
            raise ValueError(f"No rundoc value found for path '{path}' and run {plugin.run_id}")
        return _as_immutabledict_channel_map(value)

    def find_correction_value(self, correction_data, run_id, correction_file=None):
        run_id = str(run_id).zfill(6)  # Ensure run_id is always 6 digits
        value = None

        for run_range in correction_data.keys():
            if "-" in run_range:
                start_run, end_run = run_range.split("-")
                start_run = start_run.zfill(6)
                if end_run == "*":
                    # Only allow * for online corrections
                    # check if there is _dev in the filename
                    if "_dev" not in str(correction_file):
                        raise ValueError(f"Wildcard '*' is only allowed for online corrections")
                    end_run = "999999"  # Treat * as the highest possible run ID

                if start_run == "*":
                    # Only allow * for online corrections
                    # check if there is _dev in the filename
                    if "_dev" not in str(correction_file):
                        raise ValueError(f"Wildcard '*' is only allowed for online corrections")
                    start_run = "000000"

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


@export
def get_rundoc_value(
    run_id: ty.Union[str, int],
    path: str,
    detector: str = "xams",
    default: ty.Any = None,
):
    """
    Read a nested value from rundoc by dotted path.

    Example:
        get_rundoc_value("007346", "xams_bookkeeping.channel_map")
    """
    run_col = amstrax.get_mongo_collection(detector)
    doc = run_col.find_one({"number": int(run_id)})
    if not isinstance(doc, dict):
        return default

    current = doc
    for key in str(path).split("."):
        if not isinstance(current, dict):
            return default
        if key not in current:
            return default
        current = current.get(key)
    return current
