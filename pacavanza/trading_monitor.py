import asyncio
import aiohttp
import json
import logging
import signal
import threading
from functools import partial
from typing import Dict, Any
from pacavanza.modules.avanza_sse_client import AvanzaSSEClient as SSEClient
from pacavanza.modules.avanza_trading import AVANZA

logging.basicConfig(level=logging.INFO)
logging.getLogger("trading_monitor").setLevel(logging.INFO)
LOGGER = logging.getLogger("trading_monitor")


class TradingMonitor:
    """
    Implementation requirements:
    - When it is an ORDER event, we need to check if the list "orders" from orders = self._avanza.get_orders() is empty or not;
    If it is not empty, iterate the orders from list "orders", for each order:
        1. If "orderbookId" value from each order is inside instrument_list.json, then put the order in a monitoring list using "orderId" value from the order.
            a) Notice that adding orders into the monitoring list needs to be queued up, they cannot be excecuted immediately due to race condition.
            b) In the queue, process one addition of order every 3 seconds.
            c) Before finnaly put the order in the monitoring list, check orders = self._avanza.get_orders() again to see if the order is till there. If not, skip adding.
        2. For each order on the monitoring list once they are put:
            a) Start counting for 9 seconds once the order is put on the monitoring list.
            b) During this 9 seconds for each order, if a new ORDER event comes and no longer contains the order which has the same "orderbookId" value which is on the monitoring list, then remove the order from the monitoring list.
            c) If 9 seconds expires and there is still order(s) on the monitoring list, then for each order, call the backend "/trade/edit_order_follow_market" API using the same "orderId" value and "account"."accountId" value.
        3. The monitoring list needs to be safe guarded with a Lock for every operation.

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

    trading_base_url = "https://www.avanza.se/_push/trading/"

    def __init__(self, backend_url="http://localhost:8001"):
        # track tasks & clients for graceful shutdown
        self._tasks = []  # list of asyncio.Task objects we create
        self._sse_clients = {}
        self._avanza = None  # will hold the Avanza instance when created
        self._shutting_down = False
        self._loop = None

        # New components for order monitoring
        self._backend_url = backend_url
        self._http_session: aiohttp.ClientSession = None
        self._instrument_list: Dict[str, Any] = {}

        # Queue for adding orders to monitor (3s delay)
        self._add_queue = asyncio.Queue()
        self._queue_processor_task: asyncio.Task = None

        # List of actively monitored orders (orderId -> task)
        # and set of orders pending addition (in queue)
        self._monitoring_list: Dict[str, asyncio.Task] = {}
        self._pending_add_set: set[str] = set()
        self._monitoring_lock = asyncio.Lock()

    async def _load_instrument_list(self):
        """Helper to load instrument list from the backend."""
        if not self._http_session:
            raise Exception("HTTP session not initialized")

        url = f"{self._backend_url}/instrument_list.json"
        LOGGER.info(f"Loading instrument list from {url}...")
        async with self._http_session.get(url) as response:
            response.raise_for_status()  # Raise error for bad responses
            self._instrument_list = await response.json()
            LOGGER.info(
                f"Successfully loaded {len(self._instrument_list)} instruments."
            )

    async def _load_instrument_list_with_retries(self):
        """Continuously try to load the instrument list until successful."""
        while not self._shutting_down:
            try:
                await self._load_instrument_list()
                return  # Success
            except Exception as e:
                LOGGER.error(f"Failed to load instrument list: {e}. Retrying in 5s...")
                await asyncio.sleep(5)

    async def _call_edit_order_api(self, order_id: str, account_id: str):
        """Calls the backend API to edit the order."""
        if not self._http_session or self._http_session.closed:
            LOGGER.error(f"HTTP session not available, cannot edit order {order_id}.")
            return

        url = f"{self._backend_url}/trade/edit_order_follow_market"
        payload = {"orderId": order_id, "accountId": account_id}

        try:
            LOGGER.info(f"Calling edit_order_follow_market for {order_id}...")
            async with self._http_session.post(url, json=payload) as response:
                if response.status == 200:
                    LOGGER.info(
                        f"Successfully called edit_order_follow_market for {order_id}"
                    )
                else:
                    LOGGER.error(
                        f"Failed to call edit_order_follow_market for {order_id}. "
                        f"Status: {response.status}, Body: {await response.text()}"
                    )
        except Exception as e:
            LOGGER.exception(
                f"Exception calling edit_order_follow_market for {order_id}: {e}"
            )

    async def _monitor_order_task(
        self, order_id: str, account_id: str, orderbook_id: str
    ):
        """The 9-second timer task for a specific order."""
        try:
            await asyncio.sleep(9)

            # 9 seconds elapsed, check if we are still shutting down
            if self._shutting_down:
                LOGGER.info(f"Shutdown in progress, skipping API call for {order_id}.")
                return

            LOGGER.info(
                f"Order {order_id} (book {orderbook_id}) 9-second timer expired. Calling API."
            )
            await self._call_edit_order_api(order_id, account_id)

        except asyncio.CancelledError:
            LOGGER.info(
                f"Monitoring for order {order_id} (book {orderbook_id}) was cancelled (likely fulfilled)."
            )
            raise  # Re-raise to be handled by caller
        except Exception as e:
            LOGGER.exception(f"Error in monitor task for {order_id}: {e}")
        finally:
            # Always remove from monitoring list when task finishes (either by completion or cancellation)
            async with self._monitoring_lock:
                self._monitoring_list.pop(order_id, None)

    async def _process_queued_order(self, order: Dict[str, Any]):
        """Processes a single order from the queue."""
        order_id = order.get("orderId")
        account_id = order.get("account", {}).get("accountId")
        orderbook_id = order.get("orderbookId")

        if not all([order_id, account_id, orderbook_id]):
            LOGGER.warning(f"Invalid order data in queue, skipping: {order}")
            return

        try:
            # 1.c: Re-check if the order is still active
            LOGGER.debug(f"Re-checking status of queued order {order_id}...")
            current_orders_data = self._avanza.get_orders()
            all_orders = current_orders_data.get("orders", [])
            found = any(o.get("orderId") == order_id for o in all_orders)

            if not found:
                LOGGER.info(
                    f"Order {order_id} (book {orderbook_id}) no longer active, skipping monitoring."
                )
                return

            # 2: Start the 9-second monitoring task
            async with self._monitoring_lock:
                # Check again in case it was cancelled/processed by another event
                if order_id in self._monitoring_list:
                    LOGGER.debug(
                        f"Order {order_id} is already being monitored. Skipping."
                    )
                    return

                LOGGER.info(
                    f"Adding order {order_id} (book {orderbook_id}) to monitoring list (9s timer)."
                )
                monitor_task = asyncio.create_task(
                    self._monitor_order_task(order_id, account_id, orderbook_id)
                )
                self._monitoring_list[order_id] = monitor_task

        except Exception as e:
            LOGGER.exception(f"Error processing queued order {order_id}: {e}")
        finally:
            # Always remove from the pending set, regardless of outcome
            async with self._monitoring_lock:
                self._pending_add_set.discard(order_id)

    async def _queue_processor(self):
        """Background task to process the order addition queue."""
        LOGGER.info("Order queue processor started.")
        while not self._shutting_down:
            try:
                # 1.a: Wait for an order
                order = await self._add_queue.get()

                if self._shutting_down:
                    break  # Don't process if shutting down

                # 1.c: Process the order
                await self._process_queued_order(order)

                # 1.b: Wait 3 seconds before processing the next one
                await asyncio.sleep(3)

            except asyncio.CancelledError:
                LOGGER.info("Order queue processor task cancelled.")
                break
            except Exception as e:
                LOGGER.exception(f"Error in order queue processor: {e}")
                # Wait a bit before retrying
                await asyncio.sleep(1)
        LOGGER.info("Order queue processor stopped.")

    async def _callback_push(self, endpoint, _id, event, data):
        LOGGER.debug(f"{endpoint}, {event}, {data}")
        if event == "ORDER":
            try:
                orders_data = self._avanza.get_orders()
                LOGGER.debug(json.dumps(orders_data))

                active_orders = orders_data.get("orders", [])
                active_order_ids = {
                    o.get("orderId") for o in active_orders if o.get("orderId")
                }
                instrument_keys = (
                    item.get("ID") for item in self._instrument_list.values()
                )  # Relies on _load_instrument_list

                # 1. Queue new relevant orders for monitoring (Req 1)
                for order in active_orders:
                    orderbookId = order.get("orderbookId")
                    orderId = order.get("orderId")
                    if not orderId:
                        continue

                    if orderbookId in instrument_keys:
                        async with self._monitoring_lock:
                            # Add to queue ONLY if not already being monitored AND not already in the queue
                            if (
                                orderId not in self._monitoring_list
                                and orderId not in self._pending_add_set
                            ):
                                LOGGER.info(
                                    f"Queueing order {orderId} (book {orderbookId}) for monitoring."
                                )
                                self._pending_add_set.add(orderId)
                                self._add_queue.put_nowait(order)

                # 2. Handle fulfilled/cancelled orders (Req 2.b)
                async with self._monitoring_lock:
                    monitored_ids = list(self._monitoring_list.keys())  # Snapshot
                    for orderId in monitored_ids:
                        if orderId not in active_order_ids:
                            LOGGER.info(
                                f"Order {orderId} no longer in active list. Cancelling monitor."
                            )
                            task = self._monitoring_list.pop(orderId, None)
                            if task:
                                task.cancel()
                            # Also remove from pending set if it was there
                            self._pending_add_set.discard(orderId)

            except Exception as e:
                LOGGER.exception(f"Error in _callback_push ORDER handling: {e}")

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

                # Create a single http session for this Avanza instance
                self._http_session = aiohttp.ClientSession()

                # Load instrument list (with retries) before starting SSE
                await self._load_instrument_list_with_retries()
                if self._shutting_down:  # Check again after long load
                    break

                # start per-stock SSE loops (each loop handles its own reconnects)
                self._tasks = []
                for endpoint in ["orders", "stoploss", "deals"]:
                    t = asyncio.create_task(
                        self._run_sse_client_loop(self._avanza, endpoint)
                    )
                    self._tasks.append(t)

                # Start the order queue processor task
                self._queue_processor_task = asyncio.create_task(
                    self._queue_processor()
                )
                self._tasks.append(self._queue_processor_task)

                # Wait for all tasks (they are infinite loops that only stop on unexpected error)
                await asyncio.gather(*self._tasks)
            except Exception as e:
                LOGGER.error(f"Error in trading_loop: {e}. Recreating Avanza in 5s...")
                if not self._shutting_down:  # Don't sleep if shutting down
                    await asyncio.sleep(5)
            finally:
                # Cleanup http session
                if self._http_session:
                    try:
                        await self._http_session.close()
                    except Exception:
                        pass
                    finally:
                        self._http_session = None

                # Cleanup Avanza session
                try:
                    if self._avanza and hasattr(self._avanza, "close"):
                        await self._avanza.close()
                except Exception:
                    pass
                finally:
                    self._avanza = None

                # Clear tasks list
                self._tasks = []
                self._queue_processor_task = None

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

        # 2) Close HTTP session (NEW)
        if self._http_session is not None:
            try:
                await self._http_session.close()
                self._http_session = None
                LOGGER.info("HTTP session closed.")
            except Exception as e:
                LOGGER.debug(f"Exception while closing HTTP session: {e}")

        # 3) Close Avanza session if exists (await if coroutine)
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

        # 4) Cancel outstanding tasks we created and await them
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

        # 5) stop the loop (will cause run_until_complete to return)
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
