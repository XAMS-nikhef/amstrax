import json
import os
import shutil
import unittest

import strax

import amstrax
import amstrax_files


class TestAmstraxerXAMSL(unittest.TestCase):
    """
    Basic test for running amstraxer

    Run a test for the amstraxer script.
    """
    run_doc_name = 'rundoc_999999.json'
    live_data_path = './live_data/'
    test_for_context = 'xams_little'

    @classmethod
    def setUpClass(cls) -> None:
        st = amstrax.contexts.xams(init_rundb=False)
        st.storage = [strax.DataDirectory('./amstrax_data')]
        st.set_config({'live_data_dir': cls.live_data_path})
        cls.st = st
        cls.run_id = '999999'

    @classmethod
    def tearDownClass(cls) -> None:
        path_live = f'live_data/{cls.run_id}'
        if os.path.exists(path_live):
            print(f'rm {path_live}')
            shutil.rmtree(path_live)

    def setUp(self) -> None:
        self.get_test_data()

    def tearDown(self) -> None:
        path = self.st.storage[0].path
        if os.path.exists(path):
            shutil.rmtree(path)
            print(f'rm {path}')

    def test_amstraxer(self):
        self.get_test_data()
        target = [p for p in self.st._plugin_class_registry.keys() if 'raw' in p][0]
        run_id = self.run_id
        assert not self.st.is_stored(run_id, target)
        print(f'Testing for {run_id}: {target}')
        amstraxer_command = f'python {amstrax.amstrax_dir}/auto_processing/amstraxer.py'
        arguments = f'{run_id} --target {target}'
        arguments += f' --context {self.test_for_context}'
        arguments += f' --context_kwargs \'{json.dumps(dict(init_rundb=False))}\''
        arguments += f' --testing_rundoc \'{json.dumps(self.get_metadata())}\''
        arguments += f' --config_kwargs \'{json.dumps(dict(live_data_dir=self.live_data_path))}\''
        command = f'{amstraxer_command} {arguments}'
        print(command)
        return_code = os.system(command)
        assert return_code == 0, "amstraxer failed"
        assert self.st.is_stored(run_id, target)

    def test_amstraxer_for_coveralls(self):
        self.get_test_data()
        target = [p for p in self.st._plugin_class_registry.keys() if 'raw' in p][0]
        run_id = self.run_id
        assert not self.st.is_stored(run_id, target)
        args = DummyArgs()
        args.lookup.update(dict(
            run_id=run_id,
            target=target,
            context=self.test_for_context,
            context_kwargs=dict(init_rundb=False),
            config_kwargs=dict(live_data_dir=self.live_data_path),
            testing_rundoc=self.get_metadata(),
            workers=1,
            timeout=None,
        ))
        amstrax.auto_processing.amstraxer.main(args)
        assert self.st.is_stored(run_id, target)

    def get_metadata(self):
        md = amstrax_files.get_file(self.run_doc_name)
        del md['_id']
        return md

    def get_test_data(self):
        path = amstrax_files.get_abspath(f'{self.run_id}.tar')
        amstrax.common.open_test_data(path)


class TestAmstraxerXAMS(TestAmstraxerXAMSL):
    test_for_context = 'xams'

    @unittest.skip('No need to test this again')
    def test_amstraxer(self):
        return


class DummyArgs:
    """Mimic argparsing to pass to amstraxer.main"""
    lookup = {}

    def __getattr__(self, item):
        return self.lookup.get(item)

    def __setattr__(self, key, value):
        self.lookup[key] = value
