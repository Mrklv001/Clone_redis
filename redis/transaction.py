from __future__ import annotations
from typing import TYPE_CHECKING, Optional

from .resp import RespArray

if TYPE_CHECKING:
    from .commands import RedisCommand
    from .connection import RedisConnection


class RedisTransaction:
    def __init__(self, connection: RedisConnection):
        self._connection = connection
        self._command_queue: Optional[list[RedisCommand]] = None

    def activate(self) -> bool:
        """
        Activate the transaction. The returned boolean indicates whether this
        operation is successful. This will fail if the transaction has already
        been activated before (nested MULTI calls).
        """
        if self._command_queue is not None:
            return False
        self._command_queue = []
        return True

    def discard(self) -> bool:
        """
        Discard the transaction. The returned boolean indicates whether this
        operation is successful. This may fail if the transaction hasn't been
        activated yet (DISCARD without MULTI).
        """
        if self._command_queue is None:
            return False
        self._command_queue = None
        return True

    async def exec(self):
        """
        Execute the transaction. This returns an array of responses of the
        queued commands. None is returned if the transaction hasn't been
        activated yet (EXEC without multi).
        """
        if self._command_queue is None:
            return None
        # Set command_queue to None before execute(). Otherwise, the commands
        # will be queued again.
        commands, self._command_queue = self._command_queue, None
        responses = [await cmd.execute(self._connection) for cmd in commands]
        return RespArray(responses)

    def queue(self, command: RedisCommand) -> None:
        """Queue a command."""
        self._command_queue.append(command)

    @property
    def activated(self) -> bool:
        """Whether the transaction has been activated (via MULTI command)."""
        return self._command_queue is not None
