import asyncio
import json
import queue
import traceback
from asyncio import Lock

import websockets
from websockets.client import WebSocketClientProtocol
from websockets.exceptions import ConnectionClosed


def log_exception(original_function):
    def new_function(*args, **kwargs):
        try:
            return original_function(*args, **kwargs)
        except Exception:
            traceback.print_exc()

    return new_function


class WebSocketConnection:
    def __init__(
        self,
        name_suffix: str,
        route_type: str,
        endpoint_url: str,
        create_task_queue: asyncio.Queue,
        task_result_queue: asyncio.Queue,
    ):
        self._name = f"WebSocketConnection-{name_suffix}"
        self._route_type: str = route_type
        self._endpoint_url: str = endpoint_url
        self._create_task_queue: asyncio.Queue = create_task_queue
        self._task_result_queue: queue.Queue = task_result_queue

        try:
            loop = asyncio.get_event_loop()
        except Exception:
            asyncio.set_event_loop(asyncio.new_event_loop())

        try:
            self._create_connection_lock: Lock = Lock()

            self._connection: WebSocketClientProtocol = None
            loop.run_until_complete(self._create_connection())

            self._future_refs = [
                [self._create_quantum_task, loop.create_task(self._create_quantum_task())],
                [self._receive_event, loop.create_task(self._receive_event())],
            ]
        except Exception as e:
            print(self._name, e.__traceback__)

    @log_exception
    async def _create_connection(self) -> None:
        async with self._create_connection_lock:
            if self._connection and self._connection.open:
                return
            self._connection = await websockets.connect(self._endpoint_url)

    @log_exception
    async def _create_quantum_task(self):
        while True:
            # print(self._name, "_create_quantum_task() Waiting for item...")
            create_task_payload = await self._create_task_queue.get()
            # print(self._name, "Recieved item")
            create_task_payload["type"] = self._route_type

            while True:
                try:
                    # print(self._name, "Sending message", create_task_payload)
                    await self._connection.send(json.dumps(create_task_payload))
                    break
                except ConnectionClosed:
                    print(self._name, " _task_result Connection closed,", "opening a new one...")
                    await self._create_connection()

    @log_exception
    async def _receive_event(self):
        while True:
            # print(self._name, "_receive_event Waiting for event...")
            try:
                event = await self._connection.recv()
            except ConnectionClosed:
                print(self._name, " _task_result Connection closed,", "opening a new one...")
                await self._create_connection()

            await self._task_result_queue.put(event)
