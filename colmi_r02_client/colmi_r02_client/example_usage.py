#!/usr/bin/env python3
"""
Example usage of the ring monitoring script.
This shows how to customize the monitoring behavior.
"""

import asyncio
import logging
from pathlib import Path
from monitor_ring import RingMonitor

# Configure logging to see more details
logging.basicConfig(level=logging.DEBUG)

async def main():
    # Replace with your ring's MAC address
    MAC_ADDRESS = "XX:XX:XX:XX:XX:XX"  # TODO: Replace with actual MAC address
    
    if MAC_ADDRESS == "XX:XX:XX:XX:XX:XX":
        print("Please update the MAC_ADDRESS variable with your ring's actual MAC address")
        return
    
    # Create monitor with custom settings
    monitor = RingMonitor(
        mac_address=MAC_ADDRESS,
        monitor_interval=60,  # Check every minute
        record_to=Path("ring_packets.bin")  # Save raw packets
    )
    
    print(f"Starting ring monitor for {MAC_ADDRESS}")
    print("Press Ctrl+C to stop monitoring")
    
    try:
        await monitor.start_monitoring()
    except KeyboardInterrupt:
        print("\nStopping monitor...")
        monitor.stop()

if __name__ == "__main__":
    asyncio.run(main())
