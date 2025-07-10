import sys
from pathlib import Path
sys.path.insert(0, str(Path("C:/Users/willi/Desktop/ring/colmi_r02_client/colmi_r02_client/colmi_r02_client")))
from packet import make_packet

CMD_REBOOT = 8  # 0x08

REBOOT_PACKET = make_packet(CMD_REBOOT, bytearray(b"\x01"))
