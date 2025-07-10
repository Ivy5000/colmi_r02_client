#!/usr/bin/env python3
"""
Ring monitoring script that continuously collects data from a Colmi R02 ring.
"""

import asyncio
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Dict, Any
import signal
import sys
import aiohttp
import json
from dataclasses import dataclass

import sys
from pathlib import Path

# Add the colmi_r02_client directory to Python path
sys.path.insert(0, str(Path("C:/Users/willi/Desktop/ring/colmi_r02_client/colmi_r02_client/colmi_r02_client")))

from client import Client
import real_time as real_time
import firehose
from firehose import parse_firehose, SpO2, PPG, Accelerometer

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

# Home Assistant Configuration
# Update these values with your Home Assistant details
HOME_ASSISTANT_CONFIG = {
    "base_url": "http://192.168.2.125:8123",  # Your Home Assistant URL
    "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiI1ZDBkOTBlN2QxNDg0MDhhOTcxMTNiODUzYjIxYzk0MSIsImlhdCI6MTc1MjExNjk5MiwiZXhwIjoyMDY3NDc2OTkyfQ.FrPJNwWVG_ivL2gh0r2VaqnO8E6UV4ZP46GVvq6iCSg",  # Generate from HA Settings -> Profile -> Long-lived access tokens
    "device_name": "colmi_r02_ring",
    "timeout": 10
}

# Enable/disable Home Assistant integration
ENABLE_HOME_ASSISTANT = True

@dataclass
class HomeAssistantConfig:
    """Configuration for Home Assistant integration."""
    base_url: str
    access_token: str
    device_name: str = "colmi_r02_ring"
    timeout: int = 10

class HomeAssistantClient:
    """Client for sending sensor data to Home Assistant via REST API."""
    
    def __init__(self, config: HomeAssistantConfig):
        self.config = config
        self.session: Optional[aiohttp.ClientSession] = None
        self.headers = {
            "Authorization": f"Bearer {config.access_token}",
            "Content-Type": "application/json"
        }
    
    async def __aenter__(self):
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self.config.timeout)
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def _post_state(self, entity_id: str, state: Any, attributes: Optional[Dict[str, Any]] = None) -> bool:
        """Post a state update to Home Assistant."""
        if not self.session:
            logger.error("Session not initialized")
            return False
        
        url = f"{self.config.base_url}/api/states/{entity_id}"
        payload = {
            "state": str(state),
            "attributes": attributes or {}
        }
        
        try:
            async with self.session.post(url, headers=self.headers, json=payload) as response:
                if response.status in [200, 201]:
                    logger.debug(f"Successfully posted state for {entity_id}: {state}")
                    return True
                else:
                    response_text = await response.text()
                    logger.error(f"Failed to post state for {entity_id}: {response.status} - {response_text}")
                    return False
        except Exception as e:
            logger.error(f"Error posting state for {entity_id}: {e}")
            return False
    
    async def log_battery_status(self, battery_level: int, charging: bool) -> bool:
        """Log battery status to Home Assistant."""
        entity_id = f"sensor.{self.config.device_name}_battery"
        attributes = {
            "unit_of_measurement": "%",
            "device_class": "battery",
            "charging": charging,
            "friendly_name": f"{self.config.device_name.replace('_', ' ').title()} Battery",
            "last_updated": datetime.now().isoformat()
        }
        return await self._post_state(entity_id, battery_level, attributes)
    
    async def log_heart_rate(self, heart_rate: float, readings: list = None) -> bool:
        """Log heart rate data to Home Assistant."""
        entity_id = f"sensor.{self.config.device_name}_heart_rate"
        attributes = {
            "unit_of_measurement": "bpm",
            "device_class": "heart_rate",
            "friendly_name": f"{self.config.device_name.replace('_', ' ').title()} Heart Rate",
            "last_updated": datetime.now().isoformat()
        }
        if readings:
            attributes["raw_readings"] = readings
            attributes["reading_count"] = len(readings)
        return await self._post_state(entity_id, round(heart_rate, 1), attributes)
    
    async def log_spo2(self, spo2_data: SpO2) -> bool:
        """Log SpO2 data to Home Assistant."""
        entity_id = f"sensor.{self.config.device_name}_spo2"
        attributes = {
            "unit_of_measurement": "%O2",
            "device_class": "spo2",
            "friendly_name": f"{self.config.device_name.replace('_', ' ').title()} SpO2",
            "max": spo2_data.max,
            "min": spo2_data.min,
            "diff": spo2_data.diff,
            "last_updated": datetime.now().isoformat()
        }
        return await self._post_state(entity_id, spo2_data.current, attributes)
    
    async def log_ppg(self, ppg_data: PPG) -> bool:
        """Log PPG data to Home Assistant."""
        entity_id = f"sensor.{self.config.device_name}_ppg"
        attributes = {
            "unit_of_measurement": "raw",
            "friendly_name": f"{self.config.device_name.replace('_', ' ').title()} PPG",
            "max": ppg_data.max,
            "min": ppg_data.min,
            "diff": ppg_data.diff,
            "last_updated": datetime.now().isoformat()
        }
        return await self._post_state(entity_id, ppg_data.current, attributes)
    
    async def log_accelerometer(self, accel_data: Accelerometer) -> bool:
        """Log accelerometer data to Home Assistant."""
        entity_id = f"sensor.{self.config.device_name}_accelerometer"
        attributes = {
            "unit_of_measurement": "g",
            "friendly_name": f"{self.config.device_name.replace('_', ' ').title()} Accelerometer",
            "x": accel_data.x,
            "y": accel_data.y,
            "z": accel_data.z,
            "last_updated": datetime.now().isoformat()
        }
        # Calculate magnitude for the main state
        magnitude = (accel_data.x**2 + accel_data.y**2 + accel_data.z**2)**0.5
        return await self._post_state(entity_id, round(magnitude, 2), attributes)
    
    async def test_connection(self) -> bool:
        """Test connection to Home Assistant."""
        if not self.session:
            return False
        
        url = f"{self.config.base_url}/api/"
        try:
            async with self.session.get(url, headers=self.headers) as response:
                if response.status == 200:
                    logger.info("Home Assistant connection test successful")
                    return True
                else:
                    logger.error(f"Home Assistant connection test failed: {response.status}")
                    return False
        except Exception as e:
            logger.error(f"Home Assistant connection test error: {e}")
            return False

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
        self.ha_client: Optional[HomeAssistantClient] = None
        self.cycle_count = 0
        
        # Initialize Home Assistant client if enabled
        if (ENABLE_HOME_ASSISTANT and 
            HOME_ASSISTANT_CONFIG.get("access_token") != "YOUR_LONG_LIVED_ACCESS_TOKEN_HERE"):
            self.ha_config = HomeAssistantConfig(**HOME_ASSISTANT_CONFIG)
            logger.info("Home Assistant integration enabled")
        else:
            self.ha_config = None
            if ENABLE_HOME_ASSISTANT:
                logger.warning("Home Assistant integration disabled: Please update access_token in HOME_ASSISTANT_CONFIG")
            else:
                logger.info("Home Assistant integration disabled")
        
    async def start_monitoring(self):
        """Start the monitoring loop."""
        logger.info(f"Starting ring monitor for device: {self.mac_address}")
        
        try:
            # Initialize Home Assistant client if configured
            if self.ha_config:
                self.ha_client = HomeAssistantClient(self.ha_config)
                async with self.ha_client:
                    if await self.ha_client.test_connection():
                        logger.info("Home Assistant connection successful")
                    else:
                        logger.warning("Home Assistant connection failed - continuing without HA integration")
                        self.ha_client = None
            
            async with Client(self.mac_address, record_to=self.record_to) as client:
                self.client = client
                self.running = True
                
                # Reinitialize HA client for the monitoring loop if needed
                if self.ha_config and self.ha_client is None:
                    self.ha_client = HomeAssistantClient(self.ha_config)
                
                # Get device info once at startup
                await self._log_device_info()
                
                # Get initial battery status
                await self._log_battery_status()
                
                # Get heart rate log settings
                await self._log_hr_settings()
                
                logger.info(f"Starting monitoring loop (interval: {self.monitor_interval}s)")
                
                # Use Home Assistant client in context if available
                if self.ha_client:
                    async with self.ha_client:
                        while self.running:
                            await self._monitoring_cycle()
                            await asyncio.sleep(self.monitor_interval)
                else:
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
        self.cycle_count += 1
        logger.info(f"--- Starting monitoring cycle #{self.cycle_count} ---")
        
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
            
            # Log to Home Assistant if enabled (log every 10 cycles to avoid spam)
            if self.ha_client: # and self.cycle_count % 10 == 0:
                await self.ha_client.log_battery_status(
                    battery_info.battery_level, 
                    battery_info.charging
                )
                
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
                
                # Log to Home Assistant if enabled (log every cycle for HR data)
                if self.ha_client:
                    await self.ha_client.log_heart_rate(avg_hr, hr_readings)
                    
            else:
                logger.warning("Failed to get real-time heart rate readings")
                
        except Exception as e:
            logger.error(f"Failed to measure real-time heart rate: {e}")
    
    async def _get_firehose_data(self):
        """Get firehose data."""
        try:
            logger.info("Getting firehose data...")
            
            # Start firehose collection
            await self.client.send_packet(firehose.START_FIREHOSE_PACKET)
            
            # Collect data for a short period
            collected_data = []
            tries = 0
            
            while tries < 10:  # Collect for about 20 seconds (10 tries * 2 second timeout)
                tries += 1
                try:
                    # Get parsed data from the firehose queue
                    parsed_data = await asyncio.wait_for(
                        self.client.queues[firehose.CMD_FIREHOSE].get(),
                        timeout=2,
                    )
                    
                    if parsed_data:
                        collected_data.append(parsed_data)
                        
                        if isinstance(parsed_data, SpO2):
                            logger.info(f"SpO2 data: current={parsed_data.current/2.55}%, max={parsed_data.max/2.55}%, min={parsed_data.min/2.55}%, diff={parsed_data.diff}")
                            if self.ha_client:
                                await self.ha_client.log_spo2(parsed_data)
                                
                        elif isinstance(parsed_data, PPG):
                            logger.info(f"PPG data: current={parsed_data.current}, max={parsed_data.max}, min={parsed_data.min}, diff={parsed_data.diff}")
                            if self.ha_client:
                                await self.ha_client.log_ppg(parsed_data)
                                
                        elif isinstance(parsed_data, Accelerometer):
                            magnitude = ((parsed_data.x**2 + parsed_data.y**2 + parsed_data.z**2)**0.5)/1000
                            logger.info(f"Accelerometer data: x={parsed_data.x}, y={parsed_data.y}, z={parsed_data.z}, magnitude={magnitude:.2f}")
                            if self.ha_client:
                                await self.ha_client.log_accelerometer(parsed_data)
                        
                        else:
                            logger.debug(f"Unknown firehose data type: {type(parsed_data)}")
                            
                except asyncio.TimeoutError:
                    # Timeout waiting for data, continue
                    pass
                except Exception as e:
                    logger.error(f"Error processing firehose data: {e}")
                    break
                    
            # Stop firehose collection
            await self.client.send_packet(firehose.STOP_FIREHOSE_PACKET)
            
            logger.info(f"Collected {len(collected_data)} firehose data points")
            
        except Exception as e:
            logger.error(f"Failed to get firehose data: {e}")
            try:
                # Make sure to stop firehose even if there was an error
                await self.client.send_packet(firehose.STOP_FIREHOSE_PACKET)
            except:
                pass
    
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
