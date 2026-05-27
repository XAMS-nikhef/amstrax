"""Helpers for interpreting XAMS run metadata.

Structured rundoc bookkeeping is preferred, but the legacy mode strings remain
as fallback because old runs and old start paths still rely on them.
"""


LED_RUN_CLASSES = ("led",)
NAI_SOURCE_TYPES = ("nai", "nai22")
LEGACY_LED_MODE_MARKERS = ("ledcalibration", "led_calibration", "led")
LEGACY_NAI_MODE_MARKERS = ("_nai", "nai22")


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
    """Return the structured source type if present, else a legacy mode fallback."""
    bookkeeping = get_xams_bookkeeping(run_doc)
    source_type = _lower_string(bookkeeping.get("source_type"))
    if source_type:
        return source_type

    mode = _lower_string(get_run_mode(run_doc))
    if any(marker in mode for marker in LEGACY_NAI_MODE_MARKERS):
        return "nai22"
    return "none"


def is_led_run(run_doc):
    return get_run_class(run_doc) in LED_RUN_CLASSES


def has_nai_source(run_doc):
    return get_source_type(run_doc) in NAI_SOURCE_TYPES


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

