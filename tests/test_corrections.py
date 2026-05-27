import unittest
from types import SimpleNamespace

import amstrax
import strax


class TestXamsCorrections(unittest.TestCase):
    """
    Test the corrections functionality in amstrax
    """

    @classmethod
    def setUpClass(cls):
        # Initialize XAMS context with a specific correction version
        cls.st = amstrax.contexts.xams(corrections_version="ONLINE", init_rundb=False)
        cls.run_id = "002230"  # Example run_id to use in tests

        # Register a test plugin to verify the configuration setup
        class TestPlugin(strax.Plugin):
            provides = "test"
            depends_on = ("corrected_areas",)
            dtype = strax.time_fields

            __version__ = "93.0.1"

            # Custom configuration example (using amstrax XAMSConfig)
            test2 = amstrax.XAMSConfig(default="file://?filename=test_dev.json&run_id=plugin.run_id")

        cls.st.register(TestPlugin)

    def test_config_inheritance(self):
        """
        Verify that the plugin inherits the correct elife configuration and fetches the proper correction value.
        """
        plugin = self.st.get_single_plugin(self.run_id, "test")
        test2_value = plugin.test2  # Fetching test2 value

        # Check that the test2 config works and fetches the correct value
        print(f"Test2 config value: {test2_value}")
        self.assertIsNotNone(test2_value, "Test2 config should not be None")


class TestXAMSConfigCaching(unittest.TestCase):
    def test_rundoc_config_cache_is_run_aware(self):
        """
        Rundoc-backed config values must not be reused across run IDs.
        """
        cfg = amstrax.XAMSConfig(
            name="channel_map",
            default="rundoc://?path=daq_config.channel_map",
        )
        calls = []
        original = amstrax.xams_config.get_rundoc_value

        def fake_get_rundoc_value(run_id, path, detector="xams", default=None):
            calls.append((str(run_id), path, detector))
            value = int(run_id)
            return {"bottom": [value, value], "top": [value + 1, value + 1]}

        try:
            amstrax.xams_config.get_rundoc_value = fake_get_rundoc_value
            run_1 = SimpleNamespace(run_id="1", config={})
            run_2 = SimpleNamespace(run_id="2", config={})

            self.assertEqual(cfg.fetch(run_1)["bottom"], (1, 1))
            self.assertEqual(cfg.fetch(run_2)["bottom"], (2, 2))
            self.assertEqual(cfg.fetch(run_1)["bottom"], (1, 1))
            self.assertEqual(calls, [
                ("1", "daq_config.channel_map", "xams"),
                ("2", "daq_config.channel_map", "xams"),
            ])
        finally:
            amstrax.xams_config.get_rundoc_value = original

    def test_online_wildcard_correction_does_not_require_mutable_filename_state(self):
        cfg = amstrax.XAMSConfig(default=1)
        correction_data = {"1-*": 42}

        self.assertEqual(
            cfg.find_correction_value(
                correction_data,
                "2",
                correction_file="example_dev.json",
            ),
            42,
        )


if __name__ == "__main__":
    unittest.main()
