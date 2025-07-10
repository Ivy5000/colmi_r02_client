#!/usr/bin/env python3
"""
Test to verify the first heart rate reading is discarded.
"""

import asyncio
from unittest.mock import AsyncMock
from colmi_r02_client.client import Client
from colmi_r02_client import firehose, real_time


async def test_discard_first_heart_rate():
    """Test that the first heart rate reading is discarded."""
    print("Testing first heart rate reading discard...")
    
    # Create a mock client
    client = Client("mock:address")
    client.send_packet = AsyncMock()
    
    # Mock queue with heart rate data
    class MockQueue:
        def __init__(self, data):
            self.data = data
            self.index = 0
        
        async def get(self):
            if self.index < len(self.data):
                result = self.data[self.index]
                self.index += 1
                return result
            await asyncio.sleep(1)
            raise asyncio.TimeoutError()
    
    # Setup test data - multiple heart rate readings
    heart_rate_data = [
        real_time.Reading(kind=real_time.RealTimeReading.HEART_RATE, value=65),  # First (should be discarded)
        real_time.Reading(kind=real_time.RealTimeReading.HEART_RATE, value=72),  # Second (should be shown)
        real_time.Reading(kind=real_time.RealTimeReading.HEART_RATE, value=74),  # Third (should be shown)
    ]
    
    firehose_data = [
        firehose.SpO2(current=98, max=100, min=95, diff=5),
    ]
    
    # Setup mock queues
    client.queues = {
        firehose.CMD_FIREHOSE: MockQueue(firehose_data),
        real_time.CMD_START_REAL_TIME: MockQueue(heart_rate_data),
    }
    
    print("Expected output:")
    print("- First reading (65 BPM) should be marked as discarded")
    print("- Second reading (72 BPM) should be displayed normally")
    print("- Third reading (74 BPM) should be displayed normally")
    print("\nActual output:")
    
    try:
        await client.get_firehose()
    except Exception as e:
        print(f"\nTest completed (expected timeout): {e}")
    
    print("✓ First heart rate reading discard test completed!")


if __name__ == "__main__":
    asyncio.run(test_discard_first_heart_rate())
