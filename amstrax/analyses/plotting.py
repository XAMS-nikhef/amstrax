import matplotlib.pyplot as plt
import numpy as np
import strax
import amstrax  # Import your amstrax module
import matplotlib as mpl
import pandas as pd
import datetime
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import collections
from matplotlib.lines import Line2D
import warnings

export, __all__ = strax.exporter()

@amstrax.mini_analysis(requires=("raw_records","records"))
def plot_records(context, run_id, raw_records, records, raw=True, logy=False, **kwargs):
    """
    Plot raw records

    :param context: strax.Context provided by the mini-analysis wrapper
    :param run_id: Run ID of the data
    :param kwargs: Additional keyword arguments
    :return: None
    """

    if raw:
        records = raw_records
    else:
        records = records

    # Create subplots for each channel
    n_channels = max(records["channel"]) + 1

    fig, axes = plt.subplots(
        n_channels, 1, figsize=(8, 1.5 * (n_channels + 1)), sharex=True
    )
    # Plot each channel's raw record
    for i, ax in enumerate(axes):
        _records = records[records["channel"] == i]
        # For every record, plot the data (y) and time*dt (x)
        for record in _records:
            # Define timestamps for every sample of the record
            # Start from record time, then every sample is dt long
            # and add a full lenght*dt for every record_i
            time = np.linspace(0, record["length"] - 1, record["length"]) * record["dt"]
            time += record["time"] - records[0]["time"]
            if raw:
                data = -record["data"][: record["length"]]
            else:
                data = record["data"][: record["length"]]


            # Plot the data
            ax.plot(time, data)
            # if logy, set the yscale to log
            if logy:
                ax.set_yscale("log")

        ax.set_ylabel("ADC Counts")

        # add text on the upper left corner of the plot
        ax.text(
            0.05,
            0.95,
            f"Channel {i}",
            horizontalalignment="left",
            verticalalignment="top",
            transform=ax.transAxes,
        )
        # ax.set_title(f"Channel {i}")

    # remove space between subplots in the figure
    fig.subplots_adjust(wspace=0, hspace=0.1)

    axes[-1].set_ylabel("ADC Counts")
    axes[-1].legend()

    # Set the x-axis label for the last subplot
    axes[-1].set_xlabel("Time since start of peak [ns]")

    # Set the title
    # Put the title above the subplots, not inside the frame
    plt.subplots_adjust(top=0.95)
    plt.suptitle(f"raw_records {records[0]['time']} from Run ID: {run_id}")

    # Show the plot
    plt.show()


# Export the function
export(plot_records)


@amstrax.mini_analysis(requires=("peaks", "raw_records"))
def plot_peak_records(context, run_id, raw_records, peaks, **kwargs):
    """
    Plot raw records of a peak and the total waveform.

    :param context: strax.Context provided by the mini-analysis wrapper
    :param run_id: Run ID of the data
    :param peaks: Peaks for which to plot raw records
    :param kwargs: Additional keyword arguments
    :return: None
    """

    # Get the raw records for the channels in the peak
    # using strax.touching_windows

    records = raw_records

    if len(peaks) != 1:
        raise ValueError(
            "The time range you specified contains more or"
            " less than a single event. The event display "
            " only works with individual events for now."
        )

    peak = peaks[0]

    # Create subplots for each channel
    n_channels = max(records["channel"]) + 1

    fig, axes = plt.subplots(
        n_channels + 1, 1, figsize=(8, 1.5 * (n_channels + 1)), sharex=True
    )

    # Plot each channel's raw record
    for i, ax in enumerate(axes[:-1]):
        _records = records[records["channel"] == i]
        # For every record, plot the data (y) and time*dt (x)
        for record in _records:
            # Define timestamps for every sample of the record
            # Start from record time, then every sample is dt long
            # and add a full lenght*dt for every record_i

            time = np.linspace(0, record["length"] - 1, record["length"]) * record["dt"]
            time += record["time"] - peak["time"]
            data = -record["data"][: record["length"]]

            last_length = record["length"]

            # Plot the data
            ax.plot(time, data)
            plt.subplots_adjust(wspace=None, hspace=None)
            fig.subplots_adjust(0, 0, 1, 1, 0, 0)

            plt.grid()

        ax.set_ylabel("ADC Counts")

        # add text on the upper left corner of the plot
        ax.text(
            0.05,
            0.95,
            f"Channel {i}",
            horizontalalignment="left",
            verticalalignment="top",
            transform=ax.transAxes,
        )
        # ax.set_title(f"Channel {i}")

    # Plot the total waveform (sum of all channels)

    # Make peak times, give the data and the dt
    time = np.linspace(0, peak["length"] - 1, peak["length"]) * peak["dt"]
    data = peak["data"][: peak["length"]]

    # remove space between subplots in the figure
    fig.subplots_adjust(wspace=0, hspace=0.1)

    axes[-1].plot(
        time,
        data,
        label="Total Waveform",
        color="black",
    )

    axes[-1].set_ylabel("ADC Counts")
    axes[-1].legend()

    plt.grid()

    # Set the x-axis label for the last subplot
    axes[-1].set_xlabel("Time since start of peak [ns]")

    # Set the title
    # Put the title above the subplots, not inside the frame
    plt.subplots_adjust(top=0.95)
    plt.suptitle(f"Peak time: {peak['time']}, Run ID: {run_id}")

    # Show the plot
    plt.show()


# Export the function
export(plot_peak_records)


# Let's make a function to plot the area per channel
# We need two panels, one next to each other
# Left panel with 4 quadrants, for channels 1 2 3 4
# Right panel with 1 quadrants, for channels 0
@amstrax.mini_analysis(requires=("peaks",))
def plot_area_per_channel(context, run_id, peaks, **kwargs):
    fig, axes = plt.subplots(1, 2, figsize=(8, 4))

    top_quadrant_length = 1
    bottom_quadrant_length = 2

    axes = axes.flatten()

    if len(peaks) != 1:
        raise ValueError(
            "The time range you specified contains more or"
            " less than a single event. The event display "
            " only works with individual events for now."
        )

    peak = peaks[0]

    # Get the peaks
    area_per_channel = peak["area_per_channel"]

    # Plot the area per channel
    # Four quadrants for channels 1 2 3 4
    # imshow with 4 quadrants

    axes[0].imshow(
        area_per_channel[1:5].reshape(2, 2),
        cmap="viridis",
        extent=[
            -top_quadrant_length,
            top_quadrant_length,
            -top_quadrant_length,
            top_quadrant_length,
        ],
        origin="lower",
    )
    axes[1].imshow(
        area_per_channel[0].reshape(1, 1),
        cmap="viridis",
        extent=[
            -bottom_quadrant_length,
            bottom_quadrant_length,
            -bottom_quadrant_length,
            bottom_quadrant_length,
        ],
    )

    # Write the number of the channel in every element of imshow of axes[0]
    # inside a little frame such that it is more readable

    for i in range(2):
        for j in range(2):
            ch = i * 2 + j + 1
            x = j - top_quadrant_length / 2
            y = i - top_quadrant_length / 2

            t = axes[0].text(
                x,
                y,
                # format nicely in scientific notation with x10^
                f"Ch {ch} \n {area_per_channel[ch]:.1e} PE",
                horizontalalignment="center",
                verticalalignment="center",
                color="black",
            )

            t.set_bbox(dict(facecolor="white", alpha=0.5, edgecolor="black"))

    # do the same in channel 0
    t = axes[1].text(
        0,
        0,
        f"Ch 0 \n {area_per_channel[0]:.1e} PE",
        horizontalalignment="center",
        verticalalignment="center",
        color="black",
    )

    t.set_bbox(dict(facecolor="white", alpha=0.5, edgecolor="black"))

    # Set the title
    # Put the title above the subplots, not inside the frame
    plt.subplots_adjust(top=0.95)
    plt.suptitle(f"Peak time: {peak['time']}, Run ID: {run_id}")

    # Add a common colorbar for both axes
    # It needs to consider the values in both axes
    # Create an axis for the colorbar
    cax = fig.add_axes([0.95, 0.15, 0.02, 0.7])
    # Create the colorbar
    cb = plt.colorbar(
        axes[0].images[0],
        cax=cax,
    )

    # Set the label of the colorbar
    cb.set_label("Area per channel [PE]")


export(plot_area_per_channel)


@amstrax.mini_analysis(requires=("records_led",))
def plot_led_records(context, run_id, records_led, n_records=100, **kwargs):
    """
    Plots the LED records for a given run ID.

    Args:
        context (unknown): Unknown.
        run_id (int): The ID of the run to plot.
        records_led (unknown): Unknown.
        n_records (int, optional): The number of records to plot. Defaults to 100.
        **kwargs: Unknown.

    Raises:
        ValueError: If the run ID is not found in the database or if the run is not an external trigger run.

    Returns:
        None
    """
    db = amstrax.get_mongo_collection()

    rd = db.find_one({"number": int(run_id)})

    print(rd["mode"])

    if not rd:
        raise ValueError(f"Run {run_id} not found in the database.")

    if "ext_trig" not in rd["mode"]:
        raise ValueError(
            "This run doesn't look like an external trigger run, so better to avoid this plot."
        )

    st = context

    # Get the records
    records_led = records_led[0:n_records]

    records_led_config = st.get_single_plugin(run_id, "records_led").config
    led_calibration_config = st.get_single_plugin(run_id, "led_calibration").config

    # Create a figure and axis
    fig, ax = plt.subplots(figsize=(18, 6))
    colors = [k for i, k in mpl.colors.TABLEAU_COLORS.items()]

    # Plot the data
    for r in records_led[:200]:
        ax.plot(r["data"][0 : r["length"]], alpha=0.5, c=colors[int(r["channel"])])

    # Create patches for each color
    lines = [
        Line2D([0], [0], color=color, lw=2, label=f"Channel {i}")
        for i, color in enumerate(colors[:5])
    ]

    # Add the legend
    legend = ax.legend(handles=lines, frameon=True, edgecolor="black")

    # Shade regions between vertical lines and add labels
    def shade_and_label(x1, x2, color, label):
        ax.axvspan(x1, x2, color=color, alpha=0.2)
        ax.text(
            (x1 + x2) / 2,
            ax.get_ylim()[1] * 0.95,
            label,
            ha="center",
            va="top",
            color=color,
        )

    shade_and_label(
        records_led_config["baseline_window"][0],
        records_led_config["baseline_window"][1],
        "b",
        "Baseline",
    )
    shade_and_label(
        led_calibration_config["led_window"][0],
        led_calibration_config["led_window"][1],
        "r",
        "LED Signal",
    )
    shade_and_label(
        led_calibration_config["noise_window"][0],
        led_calibration_config["noise_window"][1],
        "g",
        "Noise",
    )

    # Add vertical lines
    ax.axvline(records_led_config["record_length"], linestyle="--", alpha=0.5, c="k")

    ax.axvline(
        records_led_config["baseline_window"][0], linestyle="--", alpha=0.5, c="b"
    )
    ax.axvline(
        records_led_config["baseline_window"][1], linestyle="--", alpha=0.5, c="b"
    )

    ax.axvline(
        led_calibration_config["led_window"][0], linestyle="--", alpha=0.5, c="r"
    )
    ax.axvline(
        led_calibration_config["led_window"][1], linestyle="--", alpha=0.5, c="r"
    )

    ax.axvline(
        led_calibration_config["noise_window"][0], linestyle="--", alpha=0.5, c="g"
    )
    ax.axvline(
        led_calibration_config["noise_window"][1], linestyle="--", alpha=0.5, c="g"
    )

    # Add grid, labels, and title
    ax.grid(alpha=0.4, which="both")
    ax.set_xlabel("Samples")
    ax.set_title("Data Visualization with Shaded Backgrounds")

    # Display the plot
    plt.show()


export(plot_led_records)


@amstrax.mini_analysis(requires=("led_calibration",))
def plot_led_areas(context, run_id, led_calibration, **kwargs):
    # Get led calibration

    db = amstrax.get_mongo_collection()

    rd = db.find_one({"number": int(run_id)})

    print(rd["mode"])

    if not rd:
        raise ValueError(f"Run {run_id} not found in the database.")

    if "ext_trig" not in rd["mode"]:
        raise ValueError(
            "This run doesn't look like an external trigger run, so better to avoid this plot."
        )

    fig, ax = plt.subplots(figsize=(15, 5))

    colors = [k for i, k in mpl.colors.TABLEAU_COLORS.items()]

    # Loop through each channel and plot its histogram
    for i in range(5):
        channel_data = led_calibration[led_calibration["channel"] == i]["area"]
        ax.hist(
            channel_data,
            bins=250,
            histtype="step",
            label=f"Channel {i}",
            color=colors[i],
        )

    ax.set_yscale("log")
    ax.legend(loc="upper right")
    ax.set_title("Histogram for Every Channel")
    ax.set_xlabel("Area")
    ax.set_ylabel("Frequency (log scale)")
    ax.grid(alpha=0.4, which="both")

    plt.show()


export(plot_led_areas)
