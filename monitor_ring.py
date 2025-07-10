#!/usr/bin/env python3
"""
Ring monitoring script that continuously collects data from a Colmi R02 ring.
"""

import asyncio
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
import signal
import sys

import sys
from pathlib import Path

# Add the colmi_r02_client directory to Python path
sys.path.insert(0, str(Path("C:/Users/willi/Desktop/ring/colmi_r02_client/colmi_r02_client/colmi_r02_client")))

from client import Client
import real_time as real_time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('ring_monitor.log')
    ]
)
logger = logging.getLogger(__name__)

class RingMonitor:
    def __init__(self, mac_address: str, monitor_interval: int = 30, record_to: Optional[Path] = None):
        """
        Initialize the ring monitor.
        
        Args:
            mac_address: MAC address of the ring device
            monitor_interval: Interval in seconds between measurements
            record_to: Optional path to record raw packets
        """
        self.mac_address = mac_address
        self.monitor_interval = monitor_interval
        self.record_to = record_to
        self.running = False
        self.client: Optional[Client] = None
        
    async def start_monitoring(self):
        """Start the monitoring loop."""
        logger.info(f"Starting ring monitor for device: {self.mac_address}")
        
        try:
            async with Client(self.mac_address, record_to=self.record_to) as client:
                self.client = client
                self.running = True
                
                # Get device info once at startup
                await self._log_device_info()
                
                # Get initial battery status
                await self._log_battery_status()
                
                # Get heart rate log settings
                await self._log_hr_settings()
                
                logger.info(f"Starting monitoring loop (interval: {self.monitor_interval}s)")
                
                while self.running:
                    await self._monitoring_cycle()
                    await asyncio.sleep(self.monitor_interval)
                    
        except KeyboardInterrupt:
            logger.info("Received interrupt signal, stopping monitor...")
        except Exception as e:
            logger.error(f"Error in monitoring loop: {e}")
            raise
        finally:
            self.running = False
            logger.info("Ring monitor stopped")
    
    async def _monitoring_cycle(self):
        """Perform one monitoring cycle."""
        logger.info("--- Starting monitoring cycle ---")
        
        try:
            # Get real-time heart rate
            await self._measure_realtime_heart_rate()
            
            # Get firehose data
            await self._get_firehose_data()
            
            # Get battery status periodically
            await self._log_battery_status()
            
            
        except Exception as e:
            logger.error(f"Error in monitoring cycle: {e}")
    
    async def _log_device_info(self):
        """Log device information."""
        try:
            device_info = await self.client.get_device_info()
            logger.info(f"Device info: {device_info}")
        except Exception as e:
            logger.error(f"Failed to get device info: {e}")
    
    async def _log_battery_status(self):
        """Log battery status."""
        try:
            battery_info = await self.client.get_battery()
            logger.info(f"Battery: {battery_info.battery_level}% (charging: {battery_info.charging})")
        except Exception as e:
            logger.error(f"Failed to get battery status: {e}")
    
    async def _log_hr_settings(self):
        """Log heart rate settings."""
        try:
            hr_settings = await self.client.get_heart_rate_log_settings()
            logger.info(f"HR logging settings: enabled={hr_settings.enabled}, interval={hr_settings.interval}min")
        except Exception as e:
            logger.error(f"Failed to get HR settings: {e}")
    
    async def _measure_realtime_heart_rate(self):
        """Measure real-time heart rate."""
        try:
            logger.info("Measuring real-time heart rate...")
            hr_readings = await self.client.get_realtime_reading(real_time.RealTimeReading.HEART_RATE)
            
            if hr_readings:
                avg_hr = sum(hr_readings) / len(hr_readings)
                logger.info(f"Real-time HR readings: {hr_readings} (avg: {avg_hr:.1f} bpm)")
            else:
                logger.warning("Failed to get real-time heart rate readings")
                
        except Exception as e:
            logger.error(f"Failed to measure real-time heart rate: {e}")
    
    async def _get_firehose_data(self):
        """Get firehose data."""
        try:
            logger.info("Getting firehose data...")
            await self.client.get_firehose()
        except Exception as e:
            logger.error(f"Failed to get firehose data: {e}")
    
    async def _get_heart_rate_log(self):
        """Get today's heart rate log."""
        try:
            logger.info("Getting heart rate log for today...")
            today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
            hr_log = await self.client.get_heart_rate_log(today)
            
            if hasattr(hr_log, 'readings') and hr_log.readings:
                logger.info(f"HR log: {len(hr_log.readings)} readings for today")
            else:
                logger.info("No heart rate log data for today")
                
        except Exception as e:
            logger.error(f"Failed to get heart rate log: {e}")
    
    async def _get_steps_data(self):
        """Get today's steps data."""
        try:
            logger.info("Getting steps data for today...")
            today = datetime.now(timezone.utc)
            steps_data = await self.client.get_steps(today)
            
            if hasattr(steps_data, '__len__') and len(steps_data) > 0:
                total_steps = sum(detail.steps for detail in steps_data if hasattr(detail, 'steps'))
                logger.info(f"Steps: {len(steps_data)} activities, total steps: {total_steps}")
            else:
                logger.info("No steps data for today")
                
        except Exception as e:
            logger.error(f"Failed to get steps data: {e}")
    
    def stop(self):
        """Stop the monitoring loop."""
        logger.info("Stopping monitor...")
        self.running = False


async def main():
    # Replace with your ring's MAC address
    MAC_ADDRESS = "1A:8E:08:AC:1E:97"  # TODO: Replace with actual MAC address
    
    # Optional: record raw packets to file
    record_file = Path("ring_data_capture.bin")
    
    # Create monitor instance
    monitor = RingMonitor(
        mac_address=MAC_ADDRESS,
        monitor_interval=30,  # 30 seconds between cycles
        record_to=record_file
    )
    
    # Set up signal handlers for graceful shutdown
    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}")
        monitor.stop()
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Start monitoring
    await monitor.start_monitoring()


if __name__ == "__main__":
    # Check if MAC address needs to be updated
    MAC_ADDRESS = "1A:8E:08:AC:1E:97"  # TODO: Replace with actual MAC address
    if MAC_ADDRESS == "XX:XX:XX:XX:XX:XX":
        print("Please update the MAC_ADDRESS variable in the script with your ring's actual MAC address")
        sys.exit(1)
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nMonitoring stopped by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)
