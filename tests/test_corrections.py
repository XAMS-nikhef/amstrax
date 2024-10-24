import unittest
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


if __name__ == "__main__":
    unittest.main()
