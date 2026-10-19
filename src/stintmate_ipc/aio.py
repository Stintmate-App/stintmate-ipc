"""asyncio bridge (SPEC.md is transport-level; this is the Python embedding).

The package runs on threads (overlapped pipe I/O, a sender and a reader per connection).
An asyncio app keeps that and bridges results onto its loop::

    client = Client(pipe_path("pitwall"), info, key).start()
    aclient = AsyncClient(client)
    await aclient.wait_connected(5)
    r = await aclient.call("pitwall.live_state")
    async with aclient.subscribe("pitwall.lap_completed") as events:
        async for ev in events:
            ...

Handlers offered by an asyncio app run on the pool threads; to run a coroutine there use
``asyncio.run_coroutine_threadsafe(coro, loop).result()`` inside the handler.
"""

from __future__ import annotations

import asyncio
from typing import Any

from .node import Client, ClientSubscription
from .peer import PendingCall


def wrap_call(pc: PendingCall, loop: asyncio.AbstractEventLoop | None = None
              ) -> asyncio.Future[Any]:
    """An asyncio future completed (on ``loop``) when the call completes."""
    loop = loop or asyncio.get_running_loop()
    fut: asyncio.Future[Any] = loop.create_future()

    def done(call: PendingCall) -> None:
        def settle() -> None:
            if fut.done():
                return
            err = call.exception()
            if err is not None:
                fut.set_exception(err)
            else:
                fut.set_result(call._result)
        loop.call_soon_threadsafe(settle)

    pc.add_done_callback(done)
    return fut


class AsyncSubscription:
    """An async iterator of :class:`Event` / :class:`Sample`; ``maxsize`` bounds the
    bridge queue (when full, the oldest item is dropped, so the reader never blocks)."""

    def __init__(self, client: Client, channel: str, loop: asyncio.AbstractEventLoop, *,
                 version: int = 1, options: dict[str, Any] | None = None,
                 maxsize: int = 1024) -> None:
        self._loop = loop
        self._q: asyncio.Queue[Any] = asyncio.Queue(maxsize)
        self.dropped = 0
        self.sub: ClientSubscription = client.subscribe(channel, self._push, version=version,
                                                        options=options, deliver="inline")

    def _push(self, item: Any) -> None:
        self._loop.call_soon_threadsafe(self._put, item)

    def _put(self, item: Any) -> None:
        if self._q.full():
            self._q.get_nowait()
            self.dropped += 1
        self._q.put_nowait(item)

    def __aiter__(self) -> AsyncSubscription:
        return self

    async def __anext__(self) -> Any:
        if self.sub.closed and self._q.empty():
            raise StopAsyncIteration
        return await self._q.get()

    def close(self) -> None:
        self.sub.close()

    async def __aenter__(self) -> AsyncSubscription:
        return self

    async def __aexit__(self, *exc: Any) -> None:
        self.close()


class AsyncClient:
    """Awaitable facade over a started :class:`Client` (same connection, same threads)."""

    def __init__(self, client: Client) -> None:
        self.client = client

    async def wait_connected(self, timeout: float | None = None) -> None:
        await asyncio.to_thread(self.client.wait_connected, timeout)

    async def call(self, method: str, params: Any = None, **kw: Any) -> Any:
        return await wrap_call(self.client.call_async(method, params, **kw))

    def subscribe(self, channel: str, *, version: int = 1,
                  options: dict[str, Any] | None = None, maxsize: int = 1024
                  ) -> AsyncSubscription:
        return AsyncSubscription(self.client, channel, asyncio.get_running_loop(),
                                 version=version, options=options, maxsize=maxsize)

    async def aclose(self) -> None:
        await asyncio.to_thread(self.client.close)
