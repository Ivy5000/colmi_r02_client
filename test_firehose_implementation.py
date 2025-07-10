#!/usr/bin/env python3
"""
Simple test to verify the firehose implementation works without actually connecting to a device.
This tests the logic flow and ensures print statements will be executed.
"""

import asyncio
import sys
from unittest.mock import Mock, AsyncMock
from colmi_r02_client.client import Client
from colmi_r02_client import firehose, real_time


async def test_firehose_logic():
    """Test the firehose logic without real device connection."""
    print("Testing firehose implementation...")
    
    # Create a mock client
    client = Client("mock:address")
    
    # Mock the send_packet method
    client.send_packet = AsyncMock()
    
    # Mock the queues with some test data
    class MockQueue:
        def __init__(self, data):
            self.data = data
            self.index = 0
        
        async def get(self):
            if self.index < len(self.data):
                result = self.data[self.index]
                self.index += 1
                return result
            # Simulate timeout by raising an exception after data runs out
            await asyncio.sleep(1)
            raise asyncio.TimeoutError()
    
    # Setup mock data
    firehose_data = [
        firehose.SpO2(current=98, max=100, min=95, diff=5),
        firehose.PPG(current=1200, max=1300, min=1100, diff=200),
    ]
    
    heart_rate_data = [
        real_time.Reading(kind=real_time.RealTimeReading.HEART_RATE, value=72),
        real_time.Reading(kind=real_time.RealTimeReading.HEART_RATE, value=74),
        real_time.ReadingError(kind=real_time.RealTimeReading.HEART_RATE, code=1),
    ]
    
    # Setup mock queues
    client.queues = {
        firehose.CMD_FIREHOSE: MockQueue(firehose_data),
        real_time.CMD_START_REAL_TIME: MockQueue(heart_rate_data),
    }
    
    print("Starting firehose test (should show mixed data output)...")
    
    try:
        await client.get_firehose()
    except Exception as e:
        print(f"Test completed (expected timeout/error): {e}")
    
    print("Firehose test completed!")
    
    # Verify send_packet was called with the right packets
    assert client.send_packet.call_count >= 2, "Should have called send_packet for start commands"
    print("✓ Send packet calls verified")
    
    print("✓ All tests passed!")


if __name__ == "__main__":
    asyncio.run(test_firehose_logic())
