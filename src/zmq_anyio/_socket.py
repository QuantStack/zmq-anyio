from __future__ import annotations

from contextlib import AsyncExitStack
import pickle
import selectors
from collections import deque
from functools import partial
from threading import get_ident
from typing import (
    Any,
    Callable,
    NamedTuple,
)

from anyio import (
    Event,
    TASK_STATUS_IGNORED,
    create_task_group,
    get_cancelled_exc_class,
    sleep,
    wait_readable,
    ClosedResourceError,
    notify_closing,
)
from anyio.abc import TaskGroup, TaskStatus
from anyioutils import Future, create_task

import zmq
from zmq import EVENTS, POLLIN, POLLOUT
from zmq.utils import jsonapi

try:
    DEFAULT_PROTOCOL = pickle.DEFAULT_PROTOCOL
except AttributeError:
    DEFAULT_PROTOCOL = pickle.HIGHEST_PROTOCOL


class _FutureEvent(NamedTuple):
    future: Future
    kind: str
    kwargs: dict
    msg: Any
    timer: Any


class Poller(zmq.Poller):
    """Poller that returns a Future on poll, instead of blocking."""

    raw_sockets: list[Any]

    def _watch_raw_socket(self, socket: Any, evt: int, f: Callable) -> None:
        """Schedule callback for a raw socket"""
        raise NotImplementedError()

    def _unwatch_raw_sockets(self, *sockets: Any) -> None:
        """Unschedule callback for a raw socket"""
        raise NotImplementedError()

    def apoll(self, task_group, timeout=-1) -> Future[list[tuple[Any, int]]]:
        """Return a Future for a poll event"""
        future = Future[list[tuple[Any, int]]]()
        if timeout == 0:
            try:
                result = super().poll(0)
            except Exception as e:
                future.set_exception(e)
            else:
                future.set_result(result)
            return future

        # register Future to be called as soon as any event is available on any socket
        watcher = Future[Any]()

        # watch raw sockets:
        raw_sockets: list[Any] = []

        def wake_raw(*args):
            if not watcher.done():
                watcher.set_result(None)

        # watcher.add_done_callback(lambda f: self._unwatch_raw_sockets(*raw_sockets))

        for socket, mask in self.sockets:
            if isinstance(socket, zmq.Socket):
                if not isinstance(socket, Socket):
                    raise RuntimeError(f"Not an async socket: {socket}")
                if mask & zmq.POLLIN:
                    socket._add_recv_event("poll", future=watcher)
                if mask & zmq.POLLOUT:
                    socket._add_send_event("poll", future=watcher)
            else:
                raw_sockets.append(socket)
                evt = 0
                if mask & zmq.POLLIN:
                    evt |= selectors.EVENT_READ
                if mask & zmq.POLLOUT:
                    evt |= selectors.EVENT_WRITE
                self._watch_raw_socket(socket, evt, wake_raw)

        def on_poll_ready(f):
            if future.done():
                return
            if watcher.cancelled():
                try:
                    future.cancel()
                except RuntimeError:
                    # RuntimeError may be called during teardown
                    pass
                return
            if watcher.exception():
                future.set_exception(watcher.exception())
            else:
                try:
                    result = super(Poller, self).poll(0)
                except Exception as e:
                    future.set_exception(e)
                else:
                    future.set_result(result)

        watcher.add_done_callback(on_poll_ready)

        if timeout is not None and timeout > 0:
            # schedule cancel to fire on poll timeout, if any
            async def trigger_timeout():
                await sleep(1e-3 * timeout)
                if not watcher.done():
                    watcher.set_result(None)

            if not future.done():
                timeout_handle = create_task(
                    trigger_timeout(), task_group, exception_handler=ignore_exceptions
                )

                def cancel_timeout(f):
                    timeout_handle.cancel()

                future.add_done_callback(cancel_timeout)

        def cancel_watcher(f):
            if not watcher.done():
                watcher.cancel()

        future.add_done_callback(cancel_watcher)

        return future


class _NoTimer:
    @staticmethod
    def cancel():
        pass


class Socket(zmq.Socket):
    _recv_futures = None
    _send_futures = None
    _state = 0
    _shadow_sock: zmq.Socket
    _fd = None
    _exit_stack = None
    _task_group = None
    __stack: AsyncExitStack | None = None
    _thread = None
    started = None
    stopped = None
    _starting = None
    _exited = None

    def __init__(
        self,
        context_or_socket: zmq.Context | zmq.Socket,
        socket_type: int = -1,
        task_group: TaskGroup | None = None,
        **kwargs,
    ) -> None:
        """
        Args:
            context: The context to create the socket with.
            socket_type: The socket type to create.
        """
        if isinstance(context_or_socket, zmq.Socket):
            super().__init__(shadow=context_or_socket.underlying)  # type: ignore
            self._shadow_sock = context_or_socket
            self.context = context_or_socket.context
        else:
            super().__init__(context_or_socket, socket_type, **kwargs)
            self._shadow_sock = zmq.Socket.shadow(self.underlying)

        self._recv_futures = deque()
        self._send_futures = deque()
        self._state = 0
        self._fd = self._shadow_sock.FD
        self.started = Event()
        self._exited = Event()
        self.stopped = Event()
        self._task_group = task_group
        self.__stack = None

    def get(self, key):
        result = super().get(key)
        if key == EVENTS:
            self._schedule_remaining_events(result)
        return result

    get.__doc__ = zmq.Socket.get.__doc__

    def arecv(
        self,
        flags: int = 0,
        copy: bool = True,
        track: bool = False,
    ) -> Future[bytes | zmq.Frame]:
        self._check_started()
        return self._add_recv_event("recv", dict(flags=flags, copy=copy, track=track))

    def arecv_json(
        self,
        flags: int = 0,
        **kwargs,
    ):
        future = Future[Any]()

        def callback(_future: Future) -> None:
            if _future.cancelled():
                return

            msg = _future.result()
            future.set_result(
                self._deserialize(msg, lambda buf: jsonapi.loads(buf, **kwargs))
            )

        _future = self.arecv(flags)
        _future.add_done_callback(callback)

        def _callback(future: Future) -> None:
            if future.cancelled():
                _future.cancel()

        future.add_done_callback(_callback)

        return future

    def arecv_string(self, flags: int = 0, encoding: str = "utf-8") -> Future[str]:
        """Receive a unicode string, as sent by send_string.

        Parameters
        ----------
        flags : int
            Any valid flags for :func:`Socket.recv`.
        encoding : str
            The encoding to be used

        Returns
        -------
        s : str
            The Python unicode string that arrives as encoded bytes.

        Raises
        ------
        ZMQError
            for any of the reasons :func:`Socket.recv` might fail
        """
        future = Future[Any]()

        def callback(_future: Future) -> None:
            if _future.cancelled():
                return

            msg = _future.result()
            future.set_result(self._deserialize(msg, lambda buf: buf.decode(encoding)))

        _future = self.arecv(flags)
        _future.add_done_callback(callback)

        def _callback(future: Future) -> None:
            if future.cancelled():
                _future.cancel()

        future.add_done_callback(_callback)

        return future

    def arecv_pyobj(self, flags: int = 0) -> Future[Any]:
        """Receive a Python object as a message using pickle to serialize.

        Parameters
        ----------
        flags : int
            Any valid flags for :func:`Socket.recv`.

        Returns
        -------
        obj : Python object
            The Python object that arrives as a message.

        Raises
        ------
        ZMQError
            for any of the reasons :func:`~Socket.recv` might fail
        """
        future = Future[Any]()

        def callback(_future: Future) -> None:
            if _future.cancelled():
                return

            msg = _future.result()
            future.set_result(self._deserialize(msg, pickle.loads))

        _future = self.arecv(flags)
        _future.add_done_callback(callback)

        def _callback(future: Future) -> None:
            if future.cancelled():
                _future.cancel()

        future.add_done_callback(_callback)

        return future

    def arecv_serialized(self, deserialize, flags=0, copy=True):
        """Receive a message with a custom deserialization function.

        .. versionadded:: 17

        Parameters
        ----------
        deserialize : callable
            The deserialization function to use.
            deserialize will be called with one argument: the list of frames
            returned by recv_multipart() and can return any object.
        flags : int, optional
            Any valid flags for :func:`Socket.recv`.
        copy : bool, optional
            Whether to recv bytes or Frame objects.

        Returns
        -------
        obj : object
            The object returned by the deserialization function.

        Raises
        ------
        ZMQError
            for any of the reasons :func:`~Socket.recv` might fail
        """
        future = Future()

        def callback(_future: Future) -> None:
            if _future.cancelled():
                return

            frames = _future.result()
            res = self._deserialize(frames, deserialize)
            future.set_result(res)

        _future = self.arecv_multipart(flags=flags, copy=copy)
        _future.add_done_callback(callback)

        def _callback(future: Future) -> None:
            if future.cancelled():
                _future.cancel()

        future.add_done_callback(_callback)

        return future

    def arecv_multipart(
        self,
        flags: int = 0,
        copy: bool = True,
        track: bool = False,
    ) -> Future[list[bytes] | list[zmq.Frame]]:
        self._check_started()
        return self._add_recv_event(
            "recv_multipart", dict(flags=flags, copy=copy, track=track)
        )

    def send(self, *args, **kwargs):
        if self._task_group is None:
            super().send(*args, **kwargs)
        else:
            self._task_group.start_soon(self.asend(*args, **kwargs).wait)

    def send_multipart(self, *args, **kwargs):
        if self._task_group is None:
            super().send_multipart(*args, **kwargs)
        else:
            self._task_group.start_soon(self.asend_multipart(*args, **kwargs).wait)

    def asend(
        self,
        data: bytes,
        flags: int = 0,
        copy: bool = True,
        track: bool = False,
        **kwargs: Any,
    ) -> Future[zmq.MessageTracker | None]:
        self._check_started()
        kwargs["flags"] = flags
        kwargs["copy"] = copy
        kwargs["track"] = track
        kwargs.update(dict(flags=flags, copy=copy, track=track))
        return self._add_send_event("send", msg=data, kwargs=kwargs)

    def asend_json(
        self,
        obj: Any,
        flags: int = 0,
        **kwargs,
    ) -> Future:
        send_kwargs = {}
        for key in ("routing_id", "group"):
            if key in kwargs:
                send_kwargs[key] = kwargs.pop(key)
        msg = jsonapi.dumps(obj, **kwargs)
        return self.asend(msg, flags=flags, **send_kwargs)

    def asend_string(
        self,
        u: str,
        flags: int = 0,
        copy: bool = True,
        encoding: str = "utf-8",
        **kwargs,
    ) -> Future:
        """Send a Python unicode string as a message with an encoding.

        0MQ communicates with raw bytes, so you must encode/decode
        text (str) around 0MQ.

        Parameters
        ----------
        u : str
            The unicode string to send.
        flags : int, optional
            Any valid flags for :func:`Socket.send`.
        encoding : str
            The encoding to be used
        """
        if not isinstance(u, str):
            raise TypeError("str objects only")
        return self.asend(u.encode(encoding), flags=flags, copy=copy, **kwargs)

    def asend_pyobj(
        self, obj: Any, flags: int = 0, protocol: int = DEFAULT_PROTOCOL, **kwargs
    ) -> Future:
        """Send a Python object as a message using pickle to serialize.

        Parameters
        ----------
        obj : Python object
            The Python object to send.
        flags : int
            Any valid flags for :func:`Socket.send`.
        protocol : int
            The pickle protocol number to use. The default is pickle.DEFAULT_PROTOCOL
            where defined, and pickle.HIGHEST_PROTOCOL elsewhere.
        """
        msg = pickle.dumps(obj, protocol)
        return self.asend(msg, flags=flags, **kwargs)

    def asend_serialized(self, msg, serialize, flags=0, copy=True, **kwargs) -> Future:
        """Send a message with a custom serialization function.

        .. versionadded:: 17

        Parameters
        ----------
        msg : The message to be sent. Can be any object serializable by `serialize`.
        serialize : callable
            The serialization function to use.
            serialize(msg) should return an iterable of sendable message frames
            (e.g. bytes objects), which will be passed to send_multipart.
        flags : int, optional
            Any valid flags for :func:`Socket.send`.
        copy : bool, optional
            Whether to copy the frames.

        """
        frames = serialize(msg)
        return self.asend_multipart(frames, flags=flags, copy=copy, **kwargs)

    def asend_multipart(
        self,
        msg_parts: list[bytes],
        flags: int = 0,
        copy: bool = True,
        track: bool = False,
        **kwargs,
    ) -> Future[zmq.MessageTracker | None]:
        self._check_started()
        kwargs["flags"] = flags
        kwargs["copy"] = copy
        kwargs["track"] = track
        return self._add_send_event("send_multipart", msg=msg_parts, kwargs=kwargs)

    def _deserialize(self, recvd, load):
        """Deserialize with Futures"""
        return load(recvd)

    def apoll(self, timeout=None, flags=zmq.POLLIN) -> Future:
        """poll the socket for events

        returns a Future for the poll results.
        """
        self._check_started()
        if self.closed:
            raise zmq.ZMQError(zmq.ENOTSUP)

        p = Poller()
        p.register(self, flags)
        poll_future = p.apoll(self._task_group, timeout)

        future = Future[Any]()

        def unwrap_result(f):
            if future.done():
                return
            if poll_future.cancelled():
                try:
                    future.cancel()
                except RuntimeError:
                    # RuntimeError may be called during teardown
                    pass
                return
            if f.exception():
                future.set_exception(poll_future.exception())
            else:
                evts = dict(poll_future.result())
                future.set_result(evts.get(self, 0))

        if poll_future.done():
            # hook up result if already done
            unwrap_result(poll_future)
        else:
            poll_future.add_done_callback(unwrap_result)

        def cancel_poll(future):
            """Cancel underlying poll if request has been cancelled"""
            if not poll_future.done():
                try:
                    poll_future.cancel()
                except RuntimeError:
                    # RuntimeError may be called during teardown
                    pass

        future.add_done_callback(cancel_poll)

        return future

    def _add_timeout(self, future, timeout):
        """Add a timeout for a send or recv Future"""

        def future_timeout():
            if future.done():
                # future already resolved, do nothing
                return

            # raise EAGAIN
            future.set_exception(zmq.Again())

        return self._call_later(timeout, future_timeout)

    def _call_later(self, delay, callback):
        """Schedule a function to be called later

        Override for different IOLoop implementations

        Tornado and asyncio happen to both have ioloop.call_later
        with the same signature.
        """

        async def call_later():
            await sleep(delay)
            callback()

        return create_task(
            call_later(), self._task_group, exception_handler=ignore_exceptions
        )

    @staticmethod
    def _remove_finished_future(future, event_list, event=None):
        """Make sure that futures are removed from the event list when they resolve

        Avoids delaying cleanup until the next send/recv event,
        which may never come.
        """
        # "future" instance is shared between sockets, but each socket has its own event list.
        if not event_list:
            return
        # only unconsumed events (e.g. cancelled calls)
        # will be present when this happens
        try:
            event_list.remove(event)
        except ValueError:
            # usually this will have been removed by being consumed
            return

    def _add_recv_event(self, kind, kwargs=None, future=None) -> Future:
        """Add a recv event, returning the corresponding Future"""
        f = future or Future()
        if kind.startswith("recv") and kwargs.get("flags", 0) & zmq.DONTWAIT:
            # short-circuit non-blocking calls
            recv = getattr(self._shadow_sock, kind)
            try:
                r = recv(**kwargs)
            except Exception as e:
                f.set_exception(e)
            else:
                f.set_result(r)
            return f

        timer = _NoTimer
        if hasattr(zmq, "RCVTIMEO"):
            timeout_ms = float(self._shadow_sock.rcvtimeo)
            if timeout_ms >= 0:
                timer = self._add_timeout(f, timeout_ms * 1e-3)

        # we add it to the list of futures before we add the timeout as the
        # timeout will remove the future from recv_futures to avoid leaks
        _future_event = _FutureEvent(f, kind, kwargs, msg=None, timer=timer)
        assert self._recv_futures is not None
        self._recv_futures.append(_future_event)

        if self._shadow_sock.get(EVENTS) & POLLIN:
            # recv immediately, if we can
            self._handle_recv()
        if self._recv_futures and _future_event in self._recv_futures:
            # Don't let the Future sit in _recv_events after it's done
            # no need to register this if we've already been handled
            # (i.e. immediately-resolved recv)
            f.add_done_callback(
                partial(
                    self._remove_finished_future,
                    event_list=self._recv_futures,
                    event=_future_event,
                )
            )
            self._add_io_state(POLLIN)
        return f

    def _add_send_event(self, kind, msg=None, kwargs=None, future=None) -> Future:
        """Add a send event, returning the corresponding Future"""
        f = future or Future()
        # attempt send with DONTWAIT if no futures are waiting
        # short-circuit for sends that will resolve immediately
        # only call if no send Futures are waiting
        if kind in ("send", "send_multipart") and not self._send_futures:
            flags = kwargs.get("flags", 0)
            nowait_kwargs = kwargs.copy()
            nowait_kwargs["flags"] = flags | zmq.DONTWAIT

            # short-circuit non-blocking calls
            send = getattr(self._shadow_sock, kind)
            # track if the send resolved or not
            # (EAGAIN if DONTWAIT is not set should proceed with)
            finish_early = True
            try:
                r = send(msg, **nowait_kwargs)
            except zmq.Again as e:
                if flags & zmq.DONTWAIT:
                    f.set_exception(e)
                else:
                    # EAGAIN raised and DONTWAIT not requested,
                    # proceed with async send
                    finish_early = False
            except Exception as e:
                f.set_exception(e)
            else:
                f.set_result(r)

            if finish_early:
                # short-circuit resolved, return finished Future
                # schedule wake for recv if there are any receivers waiting
                if self._recv_futures:
                    self._schedule_remaining_events()
                return f

        timer = _NoTimer
        if hasattr(zmq, "SNDTIMEO"):
            timeout_ms = float(self._shadow_sock.get(zmq.SNDTIMEO))
            if timeout_ms >= 0:
                timer = self._add_timeout(f, timeout_ms * 1e-3)

        # we add it to the list of futures before we add the timeout as the
        # timeout will remove the future from recv_futures to avoid leaks
        _future_event = _FutureEvent(f, kind, kwargs=kwargs, msg=msg, timer=timer)
        assert self._send_futures is not None
        self._send_futures.append(_future_event)
        # Don't let the Future sit in _send_futures after it's done
        f.add_done_callback(
            partial(
                self._remove_finished_future,
                event_list=self._send_futures,
                event=_future_event,
            )
        )

        self._add_io_state(POLLOUT)
        return f

    def _handle_recv(self) -> None:
        """Handle recv events"""
        if not self._shadow_sock.get(EVENTS) & POLLIN:  # type: ignore[operator]
            # event triggered, but state may have been changed between trigger and callback
            return
        f = None
        while self._recv_futures:
            f, kind, kwargs, _, timer = self._recv_futures.popleft()
            # skip any cancelled futures
            if f.done():
                f = None
            else:
                break

        if not self._recv_futures:
            self._drop_io_state(POLLIN)

        if f is None:
            return

        timer.cancel()

        if kind == "poll":
            # on poll event, just signal ready, nothing else.
            f.set_result(None)
            return
        elif kind == "recv_multipart":
            recv = self._shadow_sock.recv_multipart
        elif kind == "recv":
            recv = self._shadow_sock.recv
        else:
            raise ValueError(f"Unhandled recv event type: {kind!r}")

        kwargs["flags"] |= zmq.DONTWAIT
        try:
            result = recv(**kwargs)
        except Exception as e:
            f.set_exception(e)
        else:
            f.set_result(result)

    def _handle_send(self) -> None:
        if not self._shadow_sock.get(EVENTS) & POLLOUT:  # type: ignore[operator]
            # event triggered, but state may have been changed between trigger and callback
            return
        f = None
        while self._send_futures:
            f, kind, kwargs, msg, timer = self._send_futures.popleft()
            # skip any cancelled futures
            if f.done():
                f = None
            else:
                break

        if not self._send_futures:
            self._drop_io_state(POLLOUT)

        if f is None:
            return

        timer.cancel()

        if kind == "poll":
            # on poll event, just signal ready, nothing else.
            f.set_result(None)
            return
        elif kind == "send_multipart":
            send = self._shadow_sock.send_multipart
        elif kind == "send":
            send = self._shadow_sock.send
        else:
            raise ValueError(f"Unhandled send event type: {kind!r}")

        kwargs["flags"] |= zmq.DONTWAIT
        try:
            result = send(msg, **kwargs)
        except Exception as e:
            f.set_exception(e)
        else:
            f.set_result(result)

    # event masking from ZMQStream
    async def _handle_events(self) -> None:
        """Dispatch IO events to _handle_recv, etc."""
        if self._shadow_sock.closed:
            return

        zmq_events = self._shadow_sock.get(EVENTS)
        if zmq_events & zmq.POLLIN:  # type: ignore[operator]
            self._handle_recv()
        if zmq_events & zmq.POLLOUT:  # type: ignore[operator]
            self._handle_send()
        self._schedule_remaining_events()

    def _schedule_remaining_events(self, events=None) -> None:
        """Schedule a call to handle_events next loop iteration

        If there are still events to handle.
        """
        # edge-triggered handling
        # allow passing events in, in case this is triggered by retrieving events,
        # so we don't have to retrieve it twice.
        if self._state == 0:
            # not watching for anything, nothing to schedule
            return
        if events is None:
            events = self._shadow_sock.get(EVENTS)
        if events & self._state:
            assert self._task_group is not None
            self._task_group.start_soon(self._handle_events)

    def _add_io_state(self, state) -> None:
        """Add io_state to poller."""
        if self._state != state:
            state = self._state = self._state | state
        self._update_handler(self._state)

    def _drop_io_state(self, state) -> None:
        """Stop poller from watching an io_state."""
        if self._state & state:
            self._state = self._state & (~state)
        self._update_handler(self._state)

    def _update_handler(self, state) -> None:
        """Update IOLoop handler with state.

        zmq FD is always read-only.
        """
        self._schedule_remaining_events()

    async def __aenter__(self) -> Socket:
        if self._starting:
            return

        self._starting = True
        if self._task_group is not None:
            return self

        async with AsyncExitStack() as stack:
            self._task_group = task_group = await stack.enter_async_context(
                create_task_group()
            )
            await task_group.start(self._start)
            stack.push_async_callback(self.stop)
            self.__stack = stack.pop_all()

        return self

    async def __aexit__(self, exc_type, exc_value, exc_tb):
        if self.__stack is not None:
            try:
                return await self.__stack.__aexit__(exc_type, exc_value, exc_tb)
            finally:
                self.__stack = None
        await self.stop()

    async def start(
        self,
        *,
        task_status: TaskStatus[None] = TASK_STATUS_IGNORED,
    ) -> None:
        if self._starting:
            task_status.started()
            return

        self._starting = True
        assert self.started is not None
        if self.started.is_set():
            task_status.started()
            return

        if self._task_group is None:
            async with create_task_group() as self._task_group:
                await self._task_group.start(self._start)
                task_status.started()
        else:
            await self._task_group.start(self._start)
            task_status.started()

    async def _start(self, *, task_status: TaskStatus[None] = TASK_STATUS_IGNORED):
        assert self.started is not None
        if self.started.is_set():
            task_status.started()
            return

        assert self.started is not None
        assert self.stopped is not None
        assert self._exited is not None
        assert self._task_group is not None
        task_status.started()
        self.started.set()
        self._thread = get_ident()

        async def wait_or_cancel() -> None:
            assert self.stopped is not None
            await self.stopped.wait()
            tg.cancel_scope.cancel()

        def fileno() -> int:
            if self.closed:
                return -1
            try:
                return self._shadow_sock.fileno()
            except zmq.ZMQError:
                return -1

        try:
            while (fd := fileno()) > 0:
                async with create_task_group() as tg:
                    tg.start_soon(wait_or_cancel)
                    try:
                        await wait_readable(fd)
                    except ClosedResourceError:
                        tg.cancel_scope.cancel()
                        break
                    tg.cancel_scope.cancel()
                if self.stopped.is_set():
                    break
                await self._handle_events()
        finally:
            self._exited.set()
            self.stopped.set()

    async def stop(self):
        assert self._exited is not None
        assert self.stopped is not None

        self.stopped.set()
        try:
            await self._exited.wait()
        except get_cancelled_exc_class():
            pass
        self.close()

    def close(self, linger: int | None = None) -> None:
        fd = self._fd
        if not self.closed and fd is not None:
            notify_closing(fd)
            try:
                super().close(linger=linger)
            except BaseException:
                pass

        assert self.stopped is not None
        self.stopped.set()
        if self._task_group is not None:
            self._task_group = None

    close.__doc__ = zmq.Socket.close.__doc__

    def _check_started(self):
        if self._task_group is None:
            raise RuntimeError(
                "Socket must be used with async context manager (or `await sock.start()`)"
            )

        self._task_group.start_soon(self._start)

        assert self._thread is not None
        if self._thread != get_ident():
            raise RuntimeError("Socket must be used in the same thread")


def ignore_exceptions(exc: BaseException) -> bool:
    return True
