import numpy as np

import strax
import amstrax

export, __all__ = strax.exporter()


@export
def hit_min_amplitude(model, n_tpc_pmts=8):
    """Return hitfinder height threshold to use in processing

    :param model: Model name (str), or int to use a uniform threshold,
    or array/tuple or thresholds to use.
    """

    if isinstance(model, (int, float)):
        return np.ones(n_tpc_pmts, dtype=np.int16) * model

    if isinstance(model, (tuple, np.ndarray)):
        return model

    if model == 'pmt_commissioning_initial':
        # ADC thresholds used for the initial PMT commissioning data
        # (at least since April 28 2020, run 007305)
        result = 15 * np.ones(n_tpc_pmts, dtype=np.int16)
        return result

    if model == 'pmt_commissioning_initial_he':
        # ADC thresholds used for the initial PMT commissioning data
        # (at least since April 28 2020, run 007305)
        result = 15 * np.ones(amstrax.contexts.xnt_common_config['channel_map']['he'][1],
                              dtype=np.int16)
        return result

    raise ValueError(f"Unknown ADC threshold model {model}")
