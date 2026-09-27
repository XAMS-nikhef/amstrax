"""Helpers for interpreting XAMS run metadata.

Structured rundoc bookkeeping is preferred, but the legacy mode strings remain
as fallback because old runs and old start paths still rely on them.
"""


LED_RUN_CLASSES = ("led",)
# The Na-22 source used to be called "nai22" in the web UI (a misspelling, not the NaI detector).
SOURCE_TYPE_ALIASES = {"nai22": "na22"}
LEGACY_LED_MODE_MARKERS = ("ledcalibration", "led_calibration", "led")
LEGACY_NA22_MODE_MARKERS = ("nai22", "na22")
# The source is only a tag of the run: no processing decision may depend on it.
NAI_CHANNEL_MAP_KEY = "external"     # channel_map group of the external NaI detector (-> raw_records_ext)
# Channel map the DAQReader uses when the run doc has none (keep equal to xams_config.DEFAULT_CHANNEL_MAP)
DEFAULT_CHANNEL_MAP = {"bottom": (0, 0), "top": (1, 4), "external": (5, 5), "sipm": (6, 6), "aqmon": (40, 40)}


def _clean_string(value):
    if value is None:
        return ""
    return str(value).strip()


def _lower_string(value):
    return _clean_string(value).lower()


def _nested_dict(doc, key):
    value = doc.get(key, {}) if isinstance(doc, dict) else {}
    return value if isinstance(value, dict) else {}


def get_run_mode(run_doc):
    """Return the rundoc mode as a stripped string."""
    return _clean_string(run_doc.get("mode") if isinstance(run_doc, dict) else "")


def get_xams_bookkeeping(run_doc):
    """Return xams_bookkeeping if present and dict-like, otherwise an empty dict."""
    return _nested_dict(run_doc, "xams_bookkeeping")


def get_run_class(run_doc):
    """Return the structured run class if present, else a legacy mode fallback."""
    bookkeeping = get_xams_bookkeeping(run_doc)
    run_class = _lower_string(bookkeeping.get("run_class"))
    if run_class:
        return run_class

    mode = _lower_string(get_run_mode(run_doc))
    if any(marker in mode for marker in LEGACY_LED_MODE_MARKERS):
        return "led"
    return "science"


def get_source_type(run_doc):
    """Return the structured source type if present, else a legacy mode fallback.
    The old spelling "nai22" of the Na-22 source is returned as "na22"."""
    bookkeeping = get_xams_bookkeeping(run_doc)
    source_type = _lower_string(bookkeeping.get("source_type"))
    if source_type:
        return SOURCE_TYPE_ALIASES.get(source_type, source_type)

    mode = _lower_string(get_run_mode(run_doc))
    if any(marker in mode for marker in LEGACY_NA22_MODE_MARKERS):
        return "na22"
    return "none"


def is_led_run(run_doc):
    return get_run_class(run_doc) in LED_RUN_CLASSES


def _last_register_value(registers, board, addr):
    value = None
    for reg in registers or []:
        if not isinstance(reg, dict) or str(reg.get("board")) != str(board):
            continue
        if _lower_string(reg.get("reg")) == addr:
            try:
                value = int(str(reg.get("val")).strip(), 16)
            except (TypeError, ValueError):
                pass
    return value


def reader_channel_map(run_doc):
    """The channel map the DAQReader uses: xams_bookkeeping.channel_map, else the XAMS default."""
    channel_map = get_xams_bookkeeping(run_doc).get("channel_map")
    return channel_map if isinstance(channel_map, dict) and channel_map else DEFAULT_CHANNEL_MAP


def nai_channel_enabled(run_doc):
    """True/False if the run's DAQ config enables/disables the external NaI detector channel(s)
    (channel_map "external" group, channel enable mask 0x8120 per board); None if it cannot be decided."""
    daq_config = _nested_dict(run_doc, "daq_config")
    ext = reader_channel_map(run_doc).get(NAI_CHANNEL_MAP_KEY)
    if not ext:
        return False
    try:
        first, last = int(ext[0]), int(ext[1])
    except (TypeError, ValueError, IndexError):
        return None
    channels = daq_config.get("channels")
    if not isinstance(channels, dict) or not channels:
        return None
    decided = False
    for board, global_channels in channels.items():
        mask = _last_register_value(daq_config.get("registers"), board, "8120")
        if mask is None:
            continue
        decided = True
        for ch, global_ch in enumerate(global_channels or []):
            if first <= int(global_ch) <= last and (mask >> ch) & 1:
                return True
    return False if decided else None


def has_nai_detector(run_doc):
    """Should the external NaI detector data (raw_records_ext -> peak_basics_ext) be processed?
    Only the data decide, never the source (a tag): processed when the run's DAQ config enables the NaI
    channel. If the config cannot tell (no channel mask), it is processed whenever the channel map has an
    "external" group — without NaI data the output is simply empty."""
    enabled = nai_channel_enabled(run_doc)
    if enabled is not None:
        return enabled
    return bool(reader_channel_map(run_doc).get(NAI_CHANNEL_MAP_KEY))


def has_nai_source(run_doc):
    """Deprecated name, kept for existing scripts: use has_nai_detector (NaI detector, not the Na-22 source)."""
    return has_nai_detector(run_doc)


def run_class_source(run_doc):
    """Describe whether run_class came from structured metadata or legacy mode."""
    bookkeeping = get_xams_bookkeeping(run_doc)
    if _lower_string(bookkeeping.get("run_class")):
        return "xams_bookkeeping.run_class"
    return "legacy mode fallback"


def source_type_source(run_doc):
    """Describe whether source_type came from structured metadata or legacy mode."""
    bookkeeping = get_xams_bookkeeping(run_doc)
    if _lower_string(bookkeeping.get("source_type")):
        return "xams_bookkeeping.source_type"
    return "legacy mode fallback"
