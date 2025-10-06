import asyncio
import json
import sys
from avanza import Avanza, ChannelType
from datetime import datetime
from websockets.exceptions import ConnectionClosedError

secret = json.load(open("secret.json"))
warrant_list = json.load(open("warrant_list.json"))

if len(sys.argv) < 2:
    print("Usage: python3 extract_order.py <WARRANT_NAME>")
    exit(1)
WARRANT_NAME = sys.argv[1]

WARRANT_ID = warrant_list.get(WARRANT_NAME, {}).get("ID")
if WARRANT_ID is None:
    print("WARRANT_ID not found in warrant_list.json")
    exit(1)
WARRANT_RATIO = warrant_list.get(WARRANT_NAME, {}).get("ratio")
if WARRANT_RATIO is None:
    print("WARRANT_RATIO not found in warrant_list.json")
    exit(1)

# Global variables to store the latest values
financing_level = None


def callback_orderdepths(data):
    d = data.get("data", {})
    ts = d.get("receivedTime")
    if not ts:
        print("No receivedTime in data")
        return

    dt = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S.%f%z")
    micro = dt.microsecond // 1000  # ms
    readable_ts = f"{dt.strftime('%Y-%m-%d %H:%M:%S')}.{micro:03d} {dt.strftime('%Z')}"

    levels = d.get("levels", [])
    if not levels:
        print("No levels data")
        return

    # Track the max volume sides
    max_buy = {"volume": 0, "price": None}
    max_sell = {"volume": 0, "price": None}

    for level in levels:
        buy_side = level.get("buySide", {})
        sell_side = level.get("sellSide", {})

        if buy_side.get("volume", 0) and buy_side.get("volume", 0) > max_buy["volume"]:
            max_buy["volume"] = buy_side.get("volume", 0)
            max_buy["price"] = float(buy_side.get("price"))

        if sell_side.get("volume", 0) and sell_side.get("volume", 0) > max_sell["volume"]:
            max_sell["volume"] = sell_side.get("volume", 0)
            max_sell["price"] = float(sell_side.get("price"))

    # Ensure valid and consistent MM detection
    if (
        max_buy["price"] is None
        or max_sell["price"] is None
        or max_buy["volume"] <= 0
        or max_sell["volume"] <= 0
    ):
        print("Valid buy/sell price not found")
        return

    if max_buy["volume"] != max_sell["volume"]:
        print(f"Volume mismatch: Buy {max_buy['volume']} vs Sell {max_sell['volume']}")
        return

    print(f"{readable_ts} B: {max_buy['price']:.2f}  S: {max_sell['price']:.2f}")

async def subscribe_to_channel(avanza: Avanza):
    global financing_level
    warrant_info = avanza.get_warrant_info(WARRANT_ID)
    underlying_id = warrant_info.get('underlying', {}).get('orderbookId')
    financing_level = warrant_info.get('keyIndicators', {}).get('financingLevel')
    if underlying_id is None:
        print("Failed to get underlying ID")
    if financing_level is None:
        print("Failed to get financing level")
        return
    financing_level = float(financing_level)
    print(f"Financing Level: {financing_level}")

    await avanza.subscribe_to_id(
        ChannelType.ORDERDEPTHS,
        WARRANT_ID,
        callback_orderdepths
    )
    while True:
        await asyncio.sleep(1)  # keep it alive, but allow exceptions to bubble up

async def resilient_loop():
    while True:
        try:
            avanza = Avanza({
                'username': secret['username'],
                'password': secret['password'],
                'totpSecret': secret['totpSecret']
            })
            await subscribe_to_channel(avanza)
        except (ConnectionClosedError, TimeoutError) as e:
            print(f"Websocket closed ({e}). Reconnecting in 5 seconds...")
            await asyncio.sleep(5)
        except Exception as e:
            print(f"Error occurred: {e}. Reconnecting in 5 seconds...")
            await asyncio.sleep(5)

def main():
    asyncio.run(resilient_loop())

if __name__ == "__main__":
    main()
