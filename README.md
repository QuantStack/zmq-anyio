[![Build Status](https://github.com/QuantStack/zmq-anyio/actions/workflows/test.yml/badge.svg?query=branch%3Amain++)](https://github.com/QuantStack/zmq-anyio/actions/workflows/test.yml/badge.svg?query=branch%3Amain++)

# zmq-anyio

Asynchronous API for ZMQ using AnyIO.

## Usage

`zmq_anyio.Socket` is a subclass of `zmq.Socket`. Here is how it must be used:
- Create a `zmq_anyio.Socket` from a `zmq.Socket` or from a `zmq.Context`:
    - Create a blocking ZMQ socket using a `zmq.Context`, and pass it to an async `zmq_anyio.Socket`:
        ```py
        ctx = zmq.Context()
        sock = ctx.socket(zmq.PAIR)
        asock = zmq_anyio.Socket(sock)
        ```
    - Or create an async `zmq_anyio.Socket` using a `zmq.Context`:
        ```py
        ctx = zmq.Context()
        asock = zmq_anyio.Socket(ctx)
        ```
- Use the `zmq_anyio.Socket` with an async context manager.
- Use `arecv()` for the async API, `recv()` for the blocking API, etc.

```py
import anyio
import zmq
import zmq_anyio

ctx = zmq.Context()
sock1 = ctx.socket(zmq.PAIR)
port = sock1.bind("tcp://127.0.0.1:1234")
sock2 = ctx.socket(zmq.PAIR)
sock2.connect("tcp://127.0.0.1:1234")

# wrap the `zmq.Socket` with `zmq_anyio.Socket`:
sock1 = zmq_anyio.Socket(sock1)
sock2 = zmq_anyio.Socket(sock2)

async def main():
    async with sock1, sock2:  # use an async context manager
        await sock1.asend(b"Hello")  # use `asend` instead of `send` and await it
        sock1.asend(b", World!")  # or don't await it, it's sent in the background
        assert await sock2.arecv() == b"Hello"  # use `arecv` instead of `recv`
        future = sock2.arecv()  # or get the future and await it later
        assert await future == b", World!"

anyio.run(main)
```
