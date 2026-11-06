"""process.py: a target found already stored (made as an input of an earlier target) still gets its run-DB entry."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "amstrax" / "auto_processing_new"))
import process  # noqa: E402


class _St:
    def key_for(self, run_id, target):
        return f"{run_id}-{target}-abcdefghij"


def test_register_stored(tmp_path, monkeypatch):
    p = process.RunProcessor.__new__(process.RunProcessor)
    calls = []
    p.run_id, p.output_folder, p.st = "007790", str(tmp_path), _St()
    p.get_info_from_processed_data = lambda folder, target, st: {"lineage_hash": "abcdefghij", "n_chunks": 1, "size_mb": 0.1}
    p.add_data_entry = lambda data_type, location, **info: calls.append((data_type, location, info["lineage_hash"]))
    assert p.register_stored("sites") is False and calls == []          # not in the output folder: nothing
    (tmp_path / "007790-sites-abcdefghij").mkdir()
    assert p.register_stored("sites") is True
    assert calls == [("sites", str(tmp_path), "abcdefghij")]
