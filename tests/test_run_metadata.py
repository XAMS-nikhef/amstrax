import importlib.util
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[1] / "amstrax" / "run_metadata.py"
SPEC = importlib.util.spec_from_file_location("run_metadata", MODULE_PATH)
run_metadata = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(run_metadata)


def test_structured_led_run_class_wins_over_mode():
    run_doc = {
        "mode": "tpc",
        "xams_bookkeeping": {
            "run_class": "led",
        },
    }

    assert run_metadata.get_run_class(run_doc) == "led"
    assert run_metadata.is_led_run(run_doc)
    assert run_metadata.run_class_source(run_doc) == "xams_bookkeeping.run_class"


def test_legacy_led_mode_is_kept_as_fallback():
    run_doc = {"mode": "ext_trig_ledcalibration"}

    assert run_metadata.get_run_class(run_doc) == "led"
    assert run_metadata.is_led_run(run_doc)
    assert run_metadata.run_class_source(run_doc) == "legacy mode fallback"


def test_structured_source_type_wins_over_mode():
    run_doc = {
        "mode": "tpc",
        "xams_bookkeeping": {
            "source_type": "nai22",
        },
    }

    assert run_metadata.get_source_type(run_doc) == "nai22"
    assert run_metadata.has_nai_source(run_doc)
    assert run_metadata.source_type_source(run_doc) == "xams_bookkeeping.source_type"


def test_legacy_nai_mode_is_kept_as_fallback():
    run_doc = {"mode": "tpc_nai"}

    assert run_metadata.get_source_type(run_doc) == "nai22"
    assert run_metadata.has_nai_source(run_doc)
    assert run_metadata.source_type_source(run_doc) == "legacy mode fallback"


def test_missing_or_malformed_metadata_is_safe():
    run_doc = {
        "mode": None,
        "xams_bookkeeping": "not-a-dict",
    }

    assert run_metadata.get_run_mode(run_doc) == ""
    assert run_metadata.get_run_class(run_doc) == "science"
    assert run_metadata.get_source_type(run_doc) == "none"
    assert not run_metadata.is_led_run(run_doc)
    assert not run_metadata.has_nai_source(run_doc)

