import amstrax
import unittest
import strax
import amstrax_files
import shutil
import os


class TestStack(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        st = strax.Context()
        st.register(amstrax.daqreader.DAQReaderXamsl)
        st.storage = [strax.DataDirectory('./amstrax_data')]
        cls.run_id = '999999'
        cls.st = st

    def setUp(self) -> None:
        self.set_config()

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

    @staticmethod
    def get_md():
        return amstrax_files.get_file('rundoc_999999.json')

    def get_test_data(self):
        path = amstrax_files.get_abspath(f'{self.run_id}.tar')
        amstrax.common.open_test_data(path)

    def set_config(self):
        md = self.get_md()
        self.st.set_config(
            {'n_readout_threads': len(md['daq_config']['processing_threads']),
             'daq_input_dir': './live_data/999999',
             **amstrax.contexts.common_config_xamsl
             })