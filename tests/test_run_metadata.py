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

    # "nai22" was the old (misspelled) name of the Na-22 source
    assert run_metadata.get_source_type(run_doc) == "na22"
    # the source never decides processing: same answer as for the run without a source tag
    assert run_metadata.has_nai_source(run_doc) == run_metadata.has_nai_source({"mode": "tpc"})
    assert run_metadata.source_type_source(run_doc) == "xams_bookkeeping.source_type"


def test_legacy_na22_mode_is_kept_as_source_fallback():
    run_doc = {"mode": "tpc_nai22"}

    assert run_metadata.get_source_type(run_doc) == "na22"
    assert run_metadata.has_nai_source(run_doc) == run_metadata.has_nai_source({"mode": "tpc"})
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
    assert run_metadata.has_nai_source(run_doc)          # reader default channel map has the NaI group


def _run_with_mask(mask, source="none"):
    return {
        "mode": "Science_Rev2.2",
        "xams_bookkeeping": {"source_type": source,
                             "channel_map": {"bottom": [0, 0], "top": [1, 4], "external": [5, 5], "sipm": [6, 7]}},
        "daq_config": {
            "channels": {"27966": list(range(16))},
            "registers": [{"reg": "8120", "val": mask, "board": 27966}],
        },
    }


def test_new_na22_name():
    assert run_metadata.get_source_type({"xams_bookkeeping": {"source_type": "na22"}}) == "na22"


def test_nai_processing_follows_the_nai_channel_not_the_source():
    # Na-22 source, NaI channel (5) off: no NaI processing
    run = _run_with_mask("1f", source="na22")
    assert run_metadata.nai_channel_enabled(run) is False
    assert not run_metadata.has_nai_detector(run)
    # old spelling, NaI channel off: also no NaI processing
    assert not run_metadata.has_nai_detector(_run_with_mask("1f", source="nai22"))
    # Cs-137 source, NaI channel on: NaI processing
    run = _run_with_mask("3f", source="cs137")
    assert run_metadata.nai_channel_enabled(run) is True
    assert run_metadata.has_nai_detector(run) and run_metadata.has_nai_source(run)


def test_nai_channel_undecidable_follows_channel_map_not_source():
    for source in ("na22", "cs137", "none"):
        run = _run_with_mask("1f", source=source)
        run["daq_config"]["registers"] = []                # no channel mask in the config
        assert run_metadata.nai_channel_enabled(run) is None
        assert run_metadata.has_nai_detector(run)          # external group in the channel map -> process
        del run["xams_bookkeeping"]["channel_map"]["external"]
        assert not run_metadata.has_nai_detector(run)


def test_no_channel_map_uses_the_reader_default():
    run = {"mode": "V1725_sipmandpmt_nai_v14", "daq_config": {"channels": {"27966": list(range(16))}, "registers": []}}
    assert run_metadata.reader_channel_map(run) == run_metadata.DEFAULT_CHANNEL_MAP
    assert run_metadata.has_nai_detector(run)              # default map has the external group, mask unknown


def test_no_external_group_means_no_nai():
    run = _run_with_mask("ff", source="na22")
    del run["xams_bookkeeping"]["channel_map"]["external"]
    assert run_metadata.nai_channel_enabled(run) is False
