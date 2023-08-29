import datetime
import os
import shutil
import unittest
import strax

import amstrax
import amstrax_files


class TestXamsStack(unittest.TestCase):
    """
    Basic test for running amstrax

    In this test, we use a few chunks of live data and see if we can
    make data and everything works as expected.

    """
    run_doc_name = 'rundoc_999999.json'
    live_data_path = './live_data/'

    @classmethod
    def setUpClass(cls) -> None:
        st = amstrax.contexts.xams(init_rundb=False)
        st.storage = [strax.DataDirectory('./amstrax_data')]
        st.set_config({'live_data_dir': cls.live_data_path})
        # Run extra tests during the processing
        st.set_config({'diagnose_sorting': True, 'check_raw_record_overlaps': True})
        cls.st = st
        cls.run_id = '999999'

    def setUp(self) -> None:
        self.get_test_data()
        self.rd = self.get_metadata()
        st = amstrax.contexts.context_for_daq_reader(
            self.st,
            run_id=self.run_id,
            run_doc=self.rd,
            detector='xams')
        self.st = st

    @classmethod
    def tearDownClass(cls) -> None:
        path = cls.st.storage[0].path
        path_live = f'live_data/{cls.run_id}'
        if os.path.exists(path):
            shutil.rmtree(path)
            print(f'rm {path}')
        if os.path.exists(path_live):
            print(f'rm {path_live}')
            shutil.rmtree(path_live)

    def test_make(self):
        self.get_test_data()
        run_id = self.run_id
        for target, plugin_class in self.st._plugin_class_registry.items():
            self.st.make(run_id, target)
            if plugin_class.save_when >= strax.SaveWhen.TARGET:
                assert self.st.is_stored(run_id, target)
        with self.assertRaises(ValueError):
            # Now since we have the 'raw' data, we cannot be allowed to
            # make it again!
            amstrax.contexts.context_for_daq_reader(
                self.st,
                run_id=self.run_id,
                run_doc=self.rd,
                detector='xams')

    def get_metadata(self):
        md = amstrax_files.get_file(self.run_doc_name)
        # This is a flat dict but we need to have a datetime object,
        # since this is only a test, let's just replace it with a
        # placeholder
        md['start'] = datetime.datetime.now()
        return md

    def get_test_data(self):
        path = amstrax_files.get_abspath(f'{self.run_id}.tar')
        amstrax.common.open_test_data(path)

