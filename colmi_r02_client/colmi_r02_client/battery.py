"""
Get the battery level and charging status.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path("C:/Users/willi/Desktop/ring/colmi_r02_client/colmi_r02_client/colmi_r02_client")))
from packet import make_packet
from dataclasses import dataclass



CMD_BATTERY = 0x03

BATTERY_PACKET = make_packet(CMD_BATTERY)


@dataclass
class BatteryInfo:
    battery_level: int
    charging: bool


def parse_battery(packet: bytearray) -> BatteryInfo:
    r"""
    example: bytearray(b'\x03@\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00C')
    """
    return BatteryInfo(battery_level=packet[1], charging=bool(packet[2]))
