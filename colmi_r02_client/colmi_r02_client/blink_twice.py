import sys
from pathlib import Path
sys.path.insert(0, str(Path("C:/Users/willi/Desktop/ring/colmi_r02_client/colmi_r02_client/colmi_r02_client")))
from packet import make_packet

CMD_BLINK_TWICE = 16  # 0x10

BLINK_TWICE_PACKET = make_packet(CMD_BLINK_TWICE)
