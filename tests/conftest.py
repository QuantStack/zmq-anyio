import os
import signal
from threading import Thread

import zmq
import pytest


test_timeout_seconds = os.environ.get("ZMQ_TEST_TIMEOUT")
teardown_timeout = 10


def term_context(ctx, timeout):
    """Terminate a context with a timeout"""
    t = Thread(target=ctx.term)
    t.daemon = True
    t.start()
    t.join(timeout=timeout)
    if t.is_alive():
        # reset Context.instance, so the failure to term doesn't corrupt subsequent tests
        zmq.sugar.context.Context._instance = None
        raise RuntimeError(
            f"context {ctx} could not terminate, open sockets likely remain in test"
        )


@pytest.fixture
def sigalrm_timeout():
    """Set timeout using SIGALRM

    Avoids infinite hang in context.term for an unclean context,
    raising an error instead.
    """
    if not hasattr(signal, "SIGALRM") or not test_timeout_seconds:
        return

    def _alarm_timeout(*args):
        raise TimeoutError(f"Test did not complete in {test_timeout_seconds} seconds")

    signal.signal(signal.SIGALRM, _alarm_timeout)
    signal.alarm(test_timeout_seconds)


@pytest.fixture
def contexts(sigalrm_timeout):
    """Fixture to track contexts used in tests

    For cleanup purposes
    """
    contexts = set()
    yield contexts
    for ctx in contexts:
        try:
            term_context(ctx, teardown_timeout)
        except Exception:
            # reset Context.instance, so the failure to term doesn't corrupt subsequent tests
            zmq.sugar.context.Context._instance = None
            raise


@pytest.fixture
def context(contexts):
    ctx = zmq.Context()
    contexts.add(ctx)
    return ctx


@pytest.fixture
async def sockets(contexts):
    sockets = []
    yield sockets
    # ensure any tracked sockets get their contexts cleaned up
    for socket in sockets:
        contexts.add(socket.context)

    # close sockets
    for socket in sockets:
        if not socket.closed:
            socket.close(linger=0)


@pytest.fixture
def socket(context, sockets):
    """Fixture to create sockets, while tracking them for cleanup"""

    def new_socket(*args, **kwargs):
        s = context.socket(*args, **kwargs)
        sockets.append(s)
        return s

    return new_socket


@pytest.fixture
def create_bound_pair(socket):
    def create_bound_pair(type1=zmq.PAIR, type2=zmq.PAIR, interface='tcp://127.0.0.1'):
        """Create a bound socket pair using a random port."""
        s1 = socket(type1)
        s1.linger = 0
        port = s1.bind_to_random_port(interface)
        s2 = socket(type2)
        s2.linger = 0
        s2.connect(f'{interface}:{port}')
        return s1, s2

    return create_bound_pair


@pytest.fixture
def push_pull(create_bound_pair):
    return create_bound_pair(zmq.PUSH, zmq.PULL)


@pytest.fixture
def dealer_router(create_bound_pair):
    return create_bound_pair(zmq.DEALER, zmq.ROUTER)
