import os
import unittest

from amstrax import amstrax_dir


class TestHelpOfScripts(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.base = os.path.join(amstrax_dir, 'auto_processing')

    def _test_help_of(self, script_name: str):
        target_script = os.path.join(self.base, script_name)
        self.assertTrue(os.path.exists(target_script))
        command = f'python {target_script} --help'
        return_code = os.system(command)
        assert return_code == 0

    def test_amstraxer(self):
        self._test_help_of('amstraxer.py')

    def test_autoprocess(self):
        self._test_help_of('auto_processing.py')

    def test_process_run(self):
        self._test_help_of('process_run.py')

    def test_submit_stbc(self):
        self._test_help_of('submit_stbc.py')
