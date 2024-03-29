from immutabledict import immutabledict
import strax
import numba
import numpy as np

export, __all__ = strax.exporter()


@export
@strax.takes_config(
    strax.Option(
        "led_window",
        default=(80, 110),
        help="Window (samples) where we expect the signal in LED calibration",
    ),
    strax.Option(
        "noise_window", default=(0, 10), help="Window (samples) to analysis the noise"
    ),
)
class LEDCalibration(strax.Plugin):
    """
    Preliminary version, several parameters to set during commissioning.
    LEDCalibration returns: channel, time, dt, length, Area,
    amplitudeLED and amplitudeNOISE.
    The new variables are:
        - Area: Area computed in the given window, averaged over 6
          windows that have the same starting sample and different end
          samples.
        - amplitudeLED: peak amplitude of the LED on run in the given
          window.
        - amplitudeNOISE: amplitude of the LED on run in a window far
          from the signal one.
    """

    __version__ = "1.0.0"

    depends_on = ("records_led",)
    data_kind = "led_calibration"
    compressor = "zstd"
    parallel = "process"
    rechunk_on_save = False

    dtype = [
        ("area", np.float32, "Area averaged in integration windows"),
        ("amplitude_led", np.float32, "Amplitude in LED window"),
        ("amplitude_noise", np.float32, "Amplitude in off LED window"),
        ("channel", np.int16, "Channel"),
        ("time", np.int64, "Start time of the interval (ns since unix epoch)"),
        ("dt", np.int16, "Time resolution in ns"),
        ("length", np.int32, "Length of the interval in samples"),
    ]

    def setup(self):
        self.led_window = self.config["led_window"]
        self.noise_window = self.config["noise_window"]

    def compute(self, records_led):
        """
        The data for LED calibration are build for those PMT which belongs to channel list.
        This is used for the different ligh levels. As defaul value all the PMTs are considered.
        """

        r = records_led

        temp = np.zeros(len(r), dtype=self.dtype)
        strax.copy_to_buffer(r, temp, "_recs_to_temp_led")

        on, off = get_amplitude(r, self.led_window, self.noise_window)
        temp["amplitude_led"] = on["amplitude"]
        temp["amplitude_noise"] = off["amplitude"]

        area = get_area(r, self.led_window)
        temp["area"] = area["area"]
        return temp


_on_off_dtype = np.dtype([("channel", "int16"), ("amplitude", "float32")])


def get_amplitude(records, led_window, noise_window):
    """
    Needed for the SPE computation.
    Take the maximum in two different regions, where there is the signal and where there is not.
    """
    on = np.zeros((len(records)), dtype=_on_off_dtype)
    off = np.zeros((len(records)), dtype=_on_off_dtype)

    on["amplitude"] = np.max(records["data"][:, led_window[0] : led_window[1]], axis=1)
    on["channel"] = records["channel"]
    off["amplitude"] = np.max(
        records["data"][:, noise_window[0] : noise_window[1]], axis=1
    )
    off["channel"] = records["channel"]

    return on, off


_area_dtype = np.dtype([("channel", "int16"), ("area", "float32")])


def get_area(records, led_window):
    """
    Needed for the gain computation.
    Sum the data in the defined window to get the area.
    This is done in 6 integration window and it returns the average area.
    """
    left = led_window[0]
    end_pos = [led_window[1] + 2 * i for i in range(6)]

    Area = np.zeros((len(records)), dtype=_area_dtype)
    for right in end_pos:
        Area["area"] += records["data"][:, left:right].sum(axis=1)
    Area["channel"] = records["channel"]
    Area["area"] = Area["area"] / float(len(end_pos))

    return Area