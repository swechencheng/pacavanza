import asyncio, random
from datetime import datetime, timezone


class RandomPriceGenerator:
    def __init__(self, stock_data):
        self.stock_data = stock_data

    async def generate(self, start_price=100.0):
        price = start_price
        while True:
            await asyncio.sleep(random.uniform(0.6, 1.8))
            dt = datetime.now(timezone.utc)
            self.stock_data.update_ohlc_bar(price, dt)
            price *= random.uniform(0.9, 1.111111)
            price = max(1.0, min(price, 900.0))

    def run(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self.generate())
