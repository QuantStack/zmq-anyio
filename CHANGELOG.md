# Version history

## 0.3.12

- Update GitHub workflows and use `uv` instead of `hatch`.

## 0.3.11

- Simplify `wait_readable` loop and use `anyio.notify_closing`.

## 0.3.10

- Support `pyzmq v27`.

## 0.3.9

- Allow multiple `start`/`stop` calls.

## 0.3.8

- Make sure that `self._start` and `self.stop` calls are contained within `TaskGroup` async context manager (PR by @graingert).

## 0.3.7

- Add `test_close`.

## 0.3.6

- Try to start the socket if it has a task group.

## 0.3.5

- Ignore exceptions in all tasks.

## 0.3.4

- Ignore exceptions while reading socket.

## 0.3.3

- Don't await tasks that are already done.

## 0.3.2

- Fix teardown.

## 0.3.1

- Try to make startup/teardown more robust.

## 0.3.0

- Replace async methods with methods returning `anyioutils.Future`.

## 0.2.5

- Bump `anyio>=4.8.0` and `anyioutils>=0.5.0`.

## 0.2.4

- Use `wait_readable()` from AnyIO v4.7.0.

## 0.2.3

- Check if socket is started when calling async methods.

## 0.2.2

- Allow starting a socket multiple times.

## 0.2.1

- Update README.

## 0.2.0

- Use root task group instead of creating new ones.
- Rename `Poller.poll` to `Poller.apoll`.
- Add `arecv_string`, `arecv_pyobj`, `arecv_serialized`, and equivalent send methods.
- Add more tests and fixes.

## 0.1.3

- Use `anyio.wait_socket_readable(sock)` with a ThreadSelectorEventLoop on Windows with ProactorEventLoop.

## 0.1.2

- Block socket startup if no thread is available.

## 0.1.1

- Add `CHANGELOG.md`.
- Automatically create a GitHub release after publishing to PyPI.

## 0.1.0
