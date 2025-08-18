import json
import socket

import pytest
import zmq
from anyio import create_task_group, fail_after, move_on_after, notify_closing, sleep, to_thread, wait_all_tasks_blocked, wait_readable
from anyioutils import CancelledError, Future, create_task
from zmq_anyio import Poller, Socket

pytestmark = pytest.mark.anyio


async def test_close1(create_bound_pair):
    a, b = map(Socket, create_bound_pair(zmq.PUSH, zmq.PULL))
    with fail_after(1):
        async with create_task_group() as tg:
            await tg.start(a.start)
            await tg.start(b.start)
            await sleep(0.4)
            print(f"{a.fileno()=}")
            print(f"{b.fileno()=}")
            notify_closing(a)
            notify_closing(b)
            a.close()
            b.close()
            print(f"{a.fileno()=}")
            print(f"{b.fileno()=}")
            await sleep(0.4)


async def test_context(context):
    a, b = Socket(context, zmq.PAIR), Socket(context, zmq.PAIR)
    port = a.bind_to_random_port("tcp://127.0.0.1")
    b.connect(f'tcp://127.0.0.1:{port}')
    a.send(b"Hello")
    assert b.recv() == b"Hello"
    async with a, b:
        await a.asend(b"Hello").wait()
        assert await b.arecv().wait() == b"Hello"


async def test_arecv_multipart(create_bound_pair):
    a, b = map(Socket, create_bound_pair(zmq.PUSH, zmq.PULL))
    async with b, a:
        f = b.arecv_multipart()
        assert not f.done()
        await a.asend(b"hi").wait()
        recvd = await f.wait()
        assert recvd == [b"hi"]


async def test_arecv(create_bound_pair):
    a, b = map(Socket, create_bound_pair(zmq.PUSH, zmq.PULL))
    async with b, a:
        f1 = b.arecv()
        f2 = b.arecv()
        assert not f1.done()
        assert not f2.done()
        await a.asend_multipart([b"hi", b"there"]).wait()
        recvd = await f2.wait()
        assert f1.done()
        assert f1.result() == b"hi"
        assert recvd == b"there"


async def test_arecv_json(create_bound_pair):
    a, b = map(Socket, create_bound_pair(zmq.PUSH, zmq.PULL))
    async with b, a:
        f = b.arecv_json()
        assert not f.done()
        obj = dict(a=5)
        await a.asend_json(obj).wait()
        recvd = await f.wait()
        assert f.done()
        assert f.result() == obj
        assert recvd == obj


async def test_arecv_send(create_bound_pair):
    a, b = map(Socket, create_bound_pair(zmq.REQ, zmq.REP))
    async with b, a:
        f = b.arecv()

        def callback(future: Future) -> None:
            b.send(b", World!")

        f.add_done_callback(callback)
        a.send(b"Hello")
        assert await a.arecv().wait() == b", World!"


async def test_inproc(sockets):
    ctx = zmq.Context()
    url = "inproc://test"
    a = ctx.socket(zmq.PUSH)
    b = ctx.socket(zmq.PULL)
    a.linger = 0
    b.linger = 0
    sockets.extend([a, b])
    a.connect(url)
    b.bind(url)
    b = Socket(b)
    async with b:
        f = b.arecv()
        await sleep(0.1)
        a.send(b"hi")
        assert await f.wait() == b"hi"


@pytest.mark.parametrize("total_threads", [1, 2])
async def test_start_socket(total_threads, create_bound_pair):
    to_thread.current_default_thread_limiter().total_tokens = total_threads

    a, b = map(Socket, create_bound_pair(zmq.REQ, zmq.REP))
    a_started = False
    b_started = False

    with pytest.raises(BaseException):
        async with b:
            b_started = True
            with move_on_after(0.1):
                async with a:
                    a_started = True
                    raise RuntimeError

    assert b_started
    assert a_started
    
    to_thread.current_default_thread_limiter().total_tokens = 40

async def test_recv_multipart(create_bound_pair):
    a, b = map(Socket, create_bound_pair(zmq.PUSH, zmq.PULL))
    async with b, a:
        f = b.arecv_multipart()
        await a.asend(b"hi").wait()
        assert await f.wait() == [b"hi"]


async def test_recv(create_bound_pair):
    a, b = map(Socket, create_bound_pair(zmq.PUSH, zmq.PULL))
    async with b, a:
        f1 = b.arecv()
        f2 = b.arecv()
        await a.asend_multipart([b"hi", b"there"]).wait()
        assert await f1.wait() == b"hi"
        assert await f2.wait() == b"there"


@pytest.mark.skipif(not hasattr(zmq, "RCVTIMEO"), reason="requires RCVTIMEO")
async def test_recv_timeout(push_pull):
    a, b = map(Socket, push_pull)
    async with b, a:
        b.rcvtimeo = 100
        f1 = b.arecv()
        b.rcvtimeo = 1000
        f2 = b.arecv_multipart()
        with pytest.raises(zmq.Again):
            await f1.wait()
        await a.asend_multipart([b"hi", b"there"]).wait()
        recvd = await f2.wait()
        assert recvd == [b"hi", b"there"]


@pytest.mark.skipif(not hasattr(zmq, "SNDTIMEO"), reason="requires SNDTIMEO")
async def test_send_timeout(socket):
    s = socket(zmq.PUSH)
    s.sndtimeo = 100
    with pytest.raises(zmq.Again):
        await s.send(b"not going anywhere")


async def test_recv_string(push_pull):
    a, b = map(Socket, push_pull)
    async with b, a:
        f = b.arecv_string()
        msg = "πøøπ"
        await a.asend_string(msg).wait()
        recvd = await f.wait()
        assert recvd == msg


async def test_recv_json(push_pull):
    a, b = map(Socket, push_pull)
    async with b, a:
        f = b.arecv_json()
        obj = dict(a=5)
        await a.asend_json(obj).wait()
        recvd = await f.wait()
        assert recvd == obj


async def test_recv_json_cancelled(push_pull):
    async with create_task_group() as tg:
        a, b = map(Socket, push_pull)
        async with b, a:
            f = b.arecv_json()
            assert not f.done()
            f.cancel(raise_exception=True)
            # cycle eventloop to allow cancel events to fire
            await sleep(0)
            obj = dict(a=5)
            await a.asend_json(obj).wait()
            with pytest.raises(CancelledError):
                recvd = await f.wait()
            assert f.cancelled()
            assert f.done()
            # give it a chance to incorrectly consume the event
            events = await b.apoll(timeout=5).wait()
            assert events
            await sleep(0)
            # make sure cancelled recv didn't eat up event
            f = b.arecv_json()
            with move_on_after(5):
                recvd = await f.wait()
            assert recvd == obj


async def test_recv_pyobj(push_pull):
    a, b = map(Socket, push_pull)
    async with b, a:
        f = b.arecv_pyobj()
        obj = dict(a=5)
        await a.asend_pyobj(obj).wait()
        recvd = await f.wait()
        assert recvd == obj


async def test_custom_serialize(create_bound_pair):
    def serialize(msg):
        frames = []
        frames.extend(msg.get("identities", []))
        content = json.dumps(msg["content"]).encode("utf8")
        frames.append(content)
        return frames

    def deserialize(frames):
        identities = frames[:-1]
        content = json.loads(frames[-1].decode("utf8"))
        return {
            "identities": identities,
            "content": content,
        }

    a, b = map(Socket, create_bound_pair(zmq.DEALER, zmq.ROUTER))
    async with b, a:

        msg = {
            "content": {
                "a": 5,
                "b": "bee",
            }
        }
        await a.asend_serialized(msg, serialize).wait()
        recvd = await b.arecv_serialized(deserialize).wait()
        assert recvd["content"] == msg["content"]
        assert recvd["identities"]
        # bounce back, tests identities
        await b.asend_serialized(recvd, serialize).wait()
        r2 = await a.arecv_serialized(deserialize).wait()
        assert r2["content"] == msg["content"]
        assert not r2["identities"]


@pytest.mark.skip(reason="FIXME: sometimes raises CancelledError")
async def test_custom_serialize_error(dealer_router):
    a, b = map(Socket, dealer_router)
    async with b, a:
        await a.asend(b"not json").wait()
        with pytest.raises(TypeError):
            await b.arecv_serialized(json.loads).wait()


async def test_recv_dontwait(push_pull):
    push, pull = map(Socket, push_pull)
    async with pull, push:
        f = pull.arecv(zmq.DONTWAIT)
        with pytest.raises(zmq.Again):
            await f.wait()
        await push.asend(b"ping").wait()
        await pull.apoll().wait()  # ensure message will be waiting
        msg = await pull.arecv(zmq.DONTWAIT).wait()
        assert msg == b"ping"


async def test_recv_cancel(push_pull):
    a, b = map(Socket, push_pull)
    async with b, a:
        f1 = b.arecv()
        f2 = b.arecv_multipart()
        f1.cancel()
        assert f1.done()
        assert not f2.done()
        await a.asend_multipart([b"hi", b"there"]).wait()
        recvd = await f2.wait()
        assert f1.cancelled()
        assert f2.done()
        assert recvd == [b"hi", b"there"]


async def test_poll(push_pull):
    a, b = map(Socket, push_pull)
    async with b, a:
        f = b.apoll(timeout=0)
        await sleep(0.1)
        assert f.result() == 0

        f = b.apoll(timeout=1)
        assert not f.done()
        evt = await f.wait()

        assert evt == 0

        f = b.apoll(timeout=1000)
        assert not f.done()
        await a.asend_multipart([b"hi", b"there"]).wait()
        evt = await f.wait()
        assert evt == zmq.POLLIN
        recvd = await b.arecv_multipart().wait()
        assert recvd == [b"hi", b"there"]


async def test_poll_base_socket(sockets):
    ctx = zmq.Context()
    url = "inproc://test"
    a = Socket(ctx.socket(zmq.PUSH))
    b = Socket(ctx.socket(zmq.PULL))
    sockets.extend([a, b])
    a.bind(url)
    b.connect(url)

    poller = Poller()
    poller.register(b, zmq.POLLIN)

    async with create_task_group() as tg:
        f = poller.apoll(tg, timeout=1000)
        assert not f.done()
        a.send_multipart([b"hi", b"there"])
        evt = await f.wait()
        assert evt == [(b, zmq.POLLIN)]
        recvd = b.recv_multipart()
        assert recvd == [b"hi", b"there"]


@pytest.mark.skip(reason="FIXME: sometimes raises ZMQError")
async def test_poll_on_closed_socket(push_pull):
    a, b = push_pull
    b = Socket(b)
    async with create_task_group() as tg:
        async with b:
            f = b.apoll(timeout=1)
            await sleep(0.1)

    assert f.done()


async def test_close2(create_bound_pair):
    a, b = map(Socket, create_bound_pair(zmq.PUSH, zmq.PULL))
    with fail_after(1):
        async with create_task_group() as tg:
            await tg.start(a.start)
            await tg.start(b.start)
            await sleep(0.4)
            print(f"{a.fileno()=}")
            print(f"{b.fileno()=}")
            notify_closing(a)
            notify_closing(b)
            a.close()
            b.close()
            print(f"{a.fileno()=}")
            print(f"{b.fileno()=}")
            await sleep(0.4)


async def test_wait_readable():
    with fail_after(1):
        s1, s2 = socket.socketpair()
        with s1, s2:
            s1.setblocking(False)
            s2.setblocking(False)
            async with create_task_group() as tg:
                tg.start_soon(wait_readable, s2)
                await wait_all_tasks_blocked()
                await sleep(0.1)
                tg.cancel_scope.cancel()

        s1, s2 = socket.socketpair()
        with s1, s2:
            s1.setblocking(False)
            s2.setblocking(False)
            s1.send(b"\x00")
            await wait_readable(s2)
