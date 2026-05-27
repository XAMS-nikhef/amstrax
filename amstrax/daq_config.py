import typing as ty
import json

import numpy as np
import strax

export, __all__ = strax.exporter()


@export
def extract_channel_polarity(registers: ty.Iterable[dict]) -> dict:
    """Extract CAEN channel polarity from per-channel DPP control registers."""
    if isinstance(registers, str):
        try:
            registers = json.loads(registers)
        except json.JSONDecodeError as err:
            raise TypeError(
                "DAQ registers must be a list of register dictionaries, not an unresolved string. "
                "If this is a rundoc:// config, access it through the XAMSConfig descriptor."
            ) from err
    if isinstance(registers, dict):
        registers = registers.values()

    channel_polarity = {}
    for reg in registers or []:
        if not isinstance(reg, dict):
            raise TypeError(f"DAQ register entries must be dictionaries, got {type(reg).__name__}: {reg!r}")
        reg_addr = str(reg.get("reg", "")).lower()
        reg_val = str(reg.get("val", "")).lower()
        if not (reg_addr.startswith("1") and reg_addr.endswith("80")):
            continue
        try:
            chan_num = (int(reg_addr, 16) - 0x1080) // 0x100
            if reg_val in ("110000", "1110000"):
                channel_polarity[chan_num] = -1
            elif reg_val in ("100000", "1100000"):
                channel_polarity[chan_num] = 1
            else:
                raise ValueError(f"Unknown polarity config value '{reg_val}' for channel {chan_num}")
        except Exception as err:
            print(f"Error parsing register {reg_addr} with value {reg_val}: {err}")
    return channel_polarity


@export
def channels_in_map_group(channel_map: dict, group: str) -> set:
    """Return integer channel numbers from a channel-map group."""
    if not isinstance(channel_map, dict):
        return set()
    group_range = channel_map.get(group)
    if not isinstance(group_range, (list, tuple)) or len(group_range) != 2:
        return set()
    left, right = (int(group_range[0]), int(group_range[1]))
    return set(np.arange(left, right + 1, dtype=np.int64).tolist())
