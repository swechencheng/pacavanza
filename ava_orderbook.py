import asyncio
import json
import sys
from avanza import Avanza, ChannelType
from datetime import datetime, timezone
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
last_message_ts: datetime | None = None

# Dead Avanza websocket detection
SILENCE_THRESHOLD = 60  # seconds without messages => reconnect
WATCH_INTERVAL = 5      # how often to check the heartbeat


def callback_orderdepths(data):
    """
    This runs in the websocket callback from Avanza.
    Update last_message_ts here so the watchdog can see activity.
    """
    global last_message_ts
    d = data.get("data", {})
    ts = d.get("receivedTime")
    if not ts:
        # still update heartbeat so we know socket is alive
        last_message_ts = datetime.now(timezone.utc)
        return

    # Update heartbeat to the receive time (use local now for simplicity)
    last_message_ts = datetime.now(timezone.utc)

    # format readable timestamp from the message if needed
    try:
        dt = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S.%f%z")
        milli = dt.microsecond // 1000
        readable_ts = f"{dt.strftime('%Y-%m-%d %H:%M:%S')}.{milli:03d} {dt.strftime('%Z')}"
    except Exception:
        readable_ts = ts

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
    """
    Await subscribe_to_id (no create_task). Then attach done-callbacks to any
    new asyncio.Tasks that appeared while subscribing. Monitor heartbeat and
    clean up when something goes wrong.
    """
    global financing_level, last_message_ts

    # Fetch info
    warrant_info = avanza.get_warrant_info(WARRANT_ID)
    underlying_id = warrant_info.get("underlying", {}).get("orderbookId")
    financing_level = warrant_info.get("keyIndicators", {}).get("financingLevel")

    if underlying_id is None:
        print("Failed to get underlying ID")
    if financing_level is None:
        print("Failed to get financing level")
        return
    financing_level = float(financing_level)
    print(f"Financing Level: {financing_level}")

    # Prepare subscription-stop event for this subscription
    subscription_stopped = asyncio.Event()

    # Capture tasks before calling subscribe_to_id
    loop = asyncio.get_running_loop()
    before_tasks = set(asyncio.all_tasks())

    # This call normally starts background tasks inside the library and returns.
    await avanza.subscribe_to_id(ChannelType.ORDERDEPTHS, WARRANT_ID, callback_orderdepths)

    # Find new tasks created by subscribe_to_id (and library internals)
    after_tasks = set(asyncio.all_tasks())
    new_tasks = [t for t in (after_tasks - before_tasks) if t is not asyncio.current_task()]

    # Attach done callbacks that retrieve exceptions and signal subscription_stopped
    def _handle_task_done(task: asyncio.Task):
        try:
            exc = task.exception()
        except asyncio.CancelledError:
            return
        except Exception as e:
            # Defensive catch: reading .exception() should not raise, but just in case
            print("Error while retrieving background task exception:", e)
            return

        if exc is not None:
            print(f"Background subscription task failed: {exc!r}")
            # If it's a websocket connection error or similar, signal main loop to reconnect
            if isinstance(exc, (ConnectionClosedError, TimeoutError, OSError)):
                subscription_stopped.set()
            else:
                # still signal: we want to restart if any internal task dies
                subscription_stopped.set()

    for t in new_tasks:
        try:
            t.add_done_callback(_handle_task_done)
        except Exception as e:
            print("Could not attach done-callback to a task:", e)

    # initialize heartbeat
    last_message_ts = datetime.now(timezone.utc)

    # Watchdog loop: check heartbeat and subscription_stopped
    try:
        while not subscription_stopped.is_set():
            await asyncio.sleep(WATCH_INTERVAL)
            # If any of the new tasks finished with exception, the done-callback sets event
            if subscription_stopped.is_set():
                break

            if last_message_ts:
                age = (datetime.now(timezone.utc) - last_message_ts).total_seconds()
                if age > SILENCE_THRESHOLD:
                    print(f"No messages for {age:.0f}s (> {SILENCE_THRESHOLD}s). Assuming dead -> reconnect.")
                    subscription_stopped.set()
                    break

            # otherwise continue watching
    except asyncio.CancelledError:
        # propagate cancellation
        raise
    finally:
        # Cleanup: cancel any still-running new tasks and await them to retrieve exceptions.
        for t in new_tasks:
            if not t.done():
                try:
                    t.cancel()
                except Exception:
                    pass
        if new_tasks:
            # gather to ensure exceptions are retrieved and avoid "Task exception was never retrieved"
            await asyncio.gather(*new_tasks, return_exceptions=True)
        # clear heartbeat
        last_message_ts = None


async def resilient_loop():
    """
    Reconnect on socket failure / watchdog trigger.
    """
    while True:
        try:
            avanza = Avanza(
                {
                    "username": secret["username"],
                    "password": secret["password"],
                    "totpSecret": secret["totpSecret"],
                }
            )
            await subscribe_to_channel(avanza)
        except (ConnectionClosedError, TimeoutError, OSError) as e:
            print(f"Connection lost ({e}). Reconnecting in 5 seconds...")
            await asyncio.sleep(5)
        except Exception as e:
            print(f"Unexpected error: {e}. Reconnecting in 5 seconds...")
            await asyncio.sleep(5)


def main():
    try:
        asyncio.run(resilient_loop())
    except KeyboardInterrupt:
        print("Stopped by user.")


if __name__ == "__main__":
    main()
