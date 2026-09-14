# SPDX-License-Identifier: Apache-2.0
"""Server-side lifetimes of IPC events shared with workers."""

# Standard
import threading


class IPCEventRegistry:
    """Hold the IPC events the server shares with one worker instance.

    An exported completion event is keyed by its handle and stays held until
    the worker releases that handle (``RELEASE_EVENT``); dropping it then is
    what destroys it. A worker that disappears before releasing leaves its
    events held for the server's lifetime, bounded by its transfers in flight.
    An imported worker event is keyed by a token issued per import, because
    one worker handle is imported once per transfer that waits on it; it
    stays held until the host callback queued on the transfer stream right
    after its wait fires, which proves the wait has been consumed.
    """

    def __init__(self) -> None:
        self._exported: dict[bytes, object] = {}
        self._imported: dict[bytes, list[object]] = {}
        self._lock = threading.Lock()

    def hold_exported(self, handle: bytes, event: object) -> None:
        """Hold ``event`` until ``handle`` is released.

        Args:
            handle: Serialized handle sent to the worker.
            event: The exported completion event.
        """
        with self._lock:
            self._exported[handle] = event

    def release_exported(self, handle: bytes) -> bool:
        """Drop the event behind ``handle``; the worker is done with it.

        Args:
            handle: Handle the worker has finished with.

        Returns:
            Whether an event was held for ``handle``.
        """
        with self._lock:
            return self._exported.pop(handle, None) is not None

    def hold_imported(self, handle: bytes, event: object) -> None:
        """Hold an imported worker event until :meth:`release_imported`.

        Args:
            handle: The worker's handle the event was imported from.
            event: The imported event.
        """
        with self._lock:
            self._imported.setdefault(handle, []).append(event)

    def release_imported(self, handle: bytes) -> bool:
        """Drop one imported event for ``handle``; its stream wait has drained.

        Args:
            handle: The worker's handle.

        Returns:
            Whether an import was held for ``handle``.
        """
        with self._lock:
            events = self._imported.get(handle)
            if not events:
                return False
            events.pop()
            if not events:
                del self._imported[handle]
            return True
