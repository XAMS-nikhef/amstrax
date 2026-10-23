"""DAQReader.is_ready: which chunks of a (finished) run are read, incl. a run of a single chunk."""
from amstrax.plugins.raw_records.daqreader import DAQReader

THREAD = "xams-CELSIUS-J5010_reader_0"


def reader(tmp_path):
    r = DAQReader.__new__(DAQReader)            # only the folder logic, no context needed
    r.config = {"daq_input_dir": str(tmp_path), "readout_threads": {THREAD: 1}}
    r.n_readout_threads = 1
    return r


def folder(tmp_path, name):
    (tmp_path / name).mkdir()
    (tmp_path / name / f"{THREAD}_000000000000000").write_bytes(b"")


def test_single_chunk_run(tmp_path):
    r = reader(tmp_path)
    folder(tmp_path, "000000")
    assert not r.is_ready(0)                    # still running: more may come
    folder(tmp_path, "THE_END")
    assert r.is_ready(0)                        # ended: the only chunk is read
    assert not r.is_ready(1)


def test_multi_chunk_run(tmp_path):
    r = reader(tmp_path)
    for name in ("000000", "000000_post", "000001_pre", "000001"):
        folder(tmp_path, name)
    assert r.is_ready(0)                        # chunk 0 with its _post
    assert not r.is_ready(1)                    # last chunk waits for the end of the run
    folder(tmp_path, "THE_END")
    assert r.is_ready(1)
