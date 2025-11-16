import asyncio
import json
import logging
import signal
import threading
from functools import partial
from pacavanza.modules.avanza_sse_client import AvanzaSSEClient as SSEClient
from pacavanza.modules.avanza_trading import AVANZA, ACCOUNT_ID

logging.basicConfig(level=logging.INFO)
logging.getLogger("trading_monitor").setLevel(logging.INFO)
LOGGER = logging.getLogger("trading_monitor")


class TradingMonitor:
    trading_base_url = "https://www.avanza.se/_push/trading/"

    def __init__(self):
        # track tasks & clients for graceful shutdown
        self._tasks = []  # list of asyncio.Task objects we create
        self._sse_clients = {}
        self._avanza = None  # will hold the Avanza instance when created
        self._shutting_down = False
        self._loop = None

    async def _callback_push(self, endpoint, _id, event, data):
        LOGGER.debug(f"{endpoint}, {event}, {data}")
        if event == "ORDER":
            orders = self._avanza.get_orders()
            LOGGER.info(json.dumps(orders))
            """
            A sample orders JSON:
            {
                "orders": [
                    {
                    "account": {
                        "accountId": "5433963",
                        "name": {
                            "value": "5433963"
                        },
                        "type": {
                            "accountType": "INVESTERINGSSPARKONTO"
                        },
                        "urlParameterId": "RosI2M8iyqbf03TQfm58fw"
                    },
                    "orderId": "820200850",
                    "volume": 720,
                    "originalVolume": 720,
                    "price": 510,
                    "amount": 367200,
                    "orderbookId": "2044158",
                    "side": "SELL",
                    "validUntil": "2025-11-17",
                    "created": "2025-11-15T17:16:38",
                    "deletable": true,
                    "modifiable": true,
                    "message": "Din order skickas iväg när marknaden öppnar.",
                    "state": "ACTIVE_PENDING",
                    "stateText": "Väntande",
                    "stateMessage": "Din order skickas iväg när marknaden öppnar.",
                    "orderbook": {
                        "id": "2044158",
                        "name": "MINI L OMX AVA 1366",
                        "countryCode": "SE",
                        "currency": "SEK",
                        "instrumentType": "Warrant",
                        "volumeFactor": "1",
                        "isin": "GB00BTL17G92",
                        "mic": "FNSE"
                    },
                    "additionalParameters": {},
                    "condition": "NORMAL"
                    }
                ],
                "fundOrders": [],
                "cancelledOrders": []
            }
            """
            # TODO: Put the ACTIVE_PENDING order(s) in the monitoring list, which runs in another thread/coroutine.

    async def _run_sse_client_loop(self, avanza, endpoint):
        while True:
            # if shutdown requested, exit loop instead of creating new clients
            if self._shutting_down:
                LOGGER.info(
                    f"{endpoint} Shutdown requested — exiting _run_sse_client_loop."
                )
                break

            orderClient = None
            try:
                orderClient = SSEClient(avanza, self.trading_base_url + f"{endpoint}")
                self._sse_clients[f"{endpoint}"] = orderClient
                orderClient.add_listener(partial(self._callback_push, f"{endpoint}"))
                LOGGER.info(f"{endpoint} Starting SSE orderClient")
                await orderClient.start()
                LOGGER.info(
                    f"{endpoint} SSE orderClient stopped cleanly (will reconnect)."
                )

                # after orderClient.start() returns, check if shutdown was requested
                if self._shutting_down:
                    LOGGER.info(
                        f"{endpoint} Shutdown requested after orderClient stopped — exiting loop."
                    )
                    # attempt to remove orderClient reference and break
                    self._sse_clients.pop(f"{endpoint}", None)
                    break

            except asyncio.CancelledError:
                LOGGER.info(
                    f"{endpoint} _run_sse_client_loop cancelled: attempting orderClient stop."
                )
                try:
                    if orderClient is not None:
                        stop_fn = getattr(orderClient, "stop", None) or getattr(
                            orderClient, "close", None
                        )
                        if stop_fn:
                            res = stop_fn()
                            if asyncio.iscoroutine(res):
                                await res
                except Exception as e:
                    LOGGER.debug(
                        f"{endpoint} Exception while stopping orderClient on cancel: {e}"
                    )
                finally:
                    self._sse_clients.pop(f"{endpoint}", None)
                    raise
            except Exception as e:
                LOGGER.error(
                    f"{endpoint} SSE orderClient error: {e}. Reconnecting in 5s..."
                )
                try:
                    if orderClient is not None:
                        stop_fn = getattr(orderClient, "stop", None) or getattr(
                            orderClient, "close", None
                        )
                        if stop_fn:
                            res = stop_fn()
                            if asyncio.iscoroutine(res):
                                await res
                except Exception:
                    pass
                self._sse_clients.pop(f"{endpoint}", None)
                # if shutdown flag set, don't sleep & reconnect — break
                if self._shutting_down:
                    LOGGER.info(
                        f"{endpoint} Shutdown requested during error; exiting orderClient loop."
                    )
                    break
                await asyncio.sleep(5)

    async def trading_loop(self):
        """
        Create one Avanza instance and start an SSE client loop for every stock.
        If Avanza creation fails we retry (so the whole set reconnects together).
        """
        while True:
            if self._shutting_down:
                LOGGER.info("trading_loop: shutting down flag set — exiting loop.")
                break
            try:
                # create single Avanza instance (one login)
                self._avanza = AVANZA
                LOGGER.info("Avanza login OK.")

                # start per-stock SSE loops (each loop handles its own reconnects)
                self._tasks = []
                for endpoint in ["orders", "stoploss", "deals"]:
                    t = asyncio.create_task(
                        self._run_sse_client_loop(self._avanza, endpoint)
                    )
                    self._tasks.append(t)

                # Wait for all tasks (they are infinite loops that only stop on unexpected error)
                await asyncio.gather(*self._tasks)
            except Exception as e:
                LOGGER.error(f"Error in trading_loop: {e}. Recreating Avanza in 5s...")
                await asyncio.sleep(5)
            finally:
                try:
                    if self._avanza and hasattr(self._avanza, "close"):
                        await self._avanza.close()
                except Exception:
                    pass
                finally:
                    self._avanza = None

    async def _shutdown(self, loop, signum):
        """
        Coroutine called from signal handlers. Force-saves, stops clients, closes Avanza,
        cancels tasks and waits for them to finish before stopping the loop.
        """
        LOGGER.info(
            f"Received signal {signum}. Initiating graceful shutdown: forcing save and cancelling tasks..."
        )
        # set the flag so loops stop creating new clients
        self._shutting_down = True

        # 1) Stop SSE clients (await if they provide async stop)
        for endpoint, client in list(self._sse_clients.items()):
            try:
                LOGGER.info(f"[{endpoint}] Stopping SSE client...")
                stop_fn = getattr(client, "stop", None) or getattr(
                    client, "close", None
                )
                if stop_fn:
                    res = stop_fn()
                    if asyncio.iscoroutine(res):
                        await res
            except Exception as e:
                LOGGER.debug(f"[{endpoint}] Exception while stopping SSE client: {e}")
            finally:
                self._sse_clients.pop(endpoint, None)

        # 2) Close Avanza session if exists (await if coroutine)
        if self._avanza is not None:
            try:
                close_fn = getattr(self._avanza, "close", None)
                if close_fn:
                    res = close_fn()
                    if asyncio.iscoroutine(res):
                        await res
                self._avanza = None
            except Exception as e:
                LOGGER.debug(f"Exception while closing Avanza: {e}")

        # 3) Cancel outstanding tasks we created and await them
        # include self._tasks (per-stock loops), plus other tasks except current
        to_cancel = list(self._tasks) if self._tasks else []
        # gather other tasks (exclude current task)
        for t in asyncio.all_tasks(loop):
            if t is asyncio.current_task(loop):
                continue
            if t not in to_cancel:
                to_cancel.append(t)

        if to_cancel:
            for t in to_cancel:
                try:
                    t.cancel()
                except Exception:
                    pass

            # Wait for tasks to finish, but don't hang forever
            try:
                await asyncio.wait_for(
                    asyncio.gather(*to_cancel, return_exceptions=True), timeout=10.0
                )
            except asyncio.TimeoutError:
                LOGGER.warning(
                    "Timeout while waiting for tasks to finish during shutdown."
                )

        # 4) stop the loop (will cause run_until_complete to return)
        try:
            loop.stop()
        except Exception:
            pass

    def run(self):
        """
        Entry point: start the asyncio loop.
        Registers signal handlers to force-save on SIGINT/SIGTERM only if running in main thread.
        """
        loop = asyncio.new_event_loop()
        self._loop = loop  # save reference for external stop()
        asyncio.set_event_loop(loop)

        # create the main tasks
        main_tasks = [
            loop.create_task(self.trading_loop()),
        ]
        # keep reference so shutdown can cancel them
        self._tasks = main_tasks.copy()

        # install signal handlers only if we're running in main thread.
        if threading.current_thread() is threading.main_thread():

            def _schedule_shutdown(s):
                try:
                    asyncio.create_task(self._shutdown(loop, s))
                except Exception:
                    try:
                        asyncio.run_coroutine_threadsafe(self._shutdown(loop, s), loop)
                    except Exception:
                        pass

            for sig in (signal.SIGINT, signal.SIGTERM):
                try:
                    loop.add_signal_handler(sig, lambda s=sig: _schedule_shutdown(s))
                except Exception:
                    # if add_signal_handler fails for some reason, skip it.
                    LOGGER.debug(
                        "run(): loop.add_signal_handler failed; skipping signal handler registration."
                    )
        else:
            LOGGER.debug(
                "run(): not running in main thread — skipping signal handler registration (caller should call stop())."
            )

        try:
            loop.run_forever()
        except KeyboardInterrupt:
            LOGGER.info("KeyboardInterrupt received in run()")
        finally:
            # final cleanup: ensure tasks stopped
            try:
                loop.run_until_complete(asyncio.sleep(0.1))
            except Exception:
                pass
            try:
                loop.close()
            except Exception:
                pass

    def stop(self, timeout: float = 15.0):
        """
        Synchronous method to request graceful shutdown from another thread (e.g. main thread).
        Sets shutdown flag and schedules the async _shutdown coroutine onto the trading_monitor's loop.
        Waits up to `timeout` seconds for the shutdown coroutine to complete.
        """
        LOGGER.info(
            "Stop requested (external). Setting shutting_down flag and scheduling shutdown."
        )
        self._shutting_down = True

        if not getattr(self, "_loop", None):
            LOGGER.debug("stop(): no event loop reference; nothing to schedule.")
            return

        try:
            # schedule the coroutine on the trading_monitor's loop and wait for result (best-effort)
            fut = asyncio.run_coroutine_threadsafe(
                self._shutdown(self._loop, "external"), self._loop
            )
            try:
                fut.result(timeout=timeout)
            except Exception as e:
                LOGGER.debug(f"stop(): shutdown coroutine finished/failed/timeout: {e}")
        except Exception as e:
            LOGGER.error(
                f"stop(): failed to schedule shutdown on trading_monitor loop: {e}"
            )


if __name__ == "__main__":
    trading_monitor = TradingMonitor()
    trading_monitor.run()
