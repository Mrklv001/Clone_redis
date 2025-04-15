from __future__ import annotations
import abc
import asyncio
import itertools
from typing import TYPE_CHECKING, Optional, override

from .resp import (
    RespArray,
    RespBulkString,
    RespInteger,
    RespSerializable,
    RespSimpleError,
    RespSimpleString,
)
from .stream import RedisStream, RedisStreamEntryId
from .utils import get_current_timestamp

if TYPE_CHECKING:
    from .connection import RedisConnection


class RedisCommand(RespSerializable, abc.ABC):
    def __init__(self, argv: list[str]) -> None:
        self._argv = argv

    def serialize(self) -> bytes:
        return RespArray([RespBulkString(arg) for arg in self._argv]).serialize()

    async def execute(self, connection: RedisConnection) -> Optional[RespSerializable]:
        """
        Execute the command and return the response. If the command should be
        queued instead, queue it and return "QUEUED" encoded as a simple string.
        """
        from .connection import ConnectionType

        if self._should_be_queued(connection):
            connection.transaction.queue(command=self)
            response = RespSimpleString("QUEUED")
        else:
            response = await self._execute(connection)

        if connection.type is ConnectionType.MASTER:
            connection.server.master_repl_offset += len(self.serialize())

        if self._should_be_propogated():
            await connection.server.send_command_to_replicas(command=self)
            connection.propogate_offset += len(self.serialize())

        return response if self._has_response(connection) else None

    def _should_be_queued(self, connection: RedisConnection) -> bool:
        """Whether the command should be queued."""
        return connection.transaction.activated

    def _should_be_propogated(self) -> bool:
        """Whether the command should be propogated."""
        return False

    def _has_response(self, connection: RedisConnection) -> bool:
        """Whether a response should be sent to connection."""
        from .connection import ConnectionType

        return connection.type is not ConnectionType.MASTER

    @abc.abstractmethod
    async def _execute(self, connection: RedisConnection) -> RespSerializable:
        """Execute the command and return the response."""
        raise NotImplementedError


class ConfigCommand(RedisCommand):
    async def _execute(self, connection: RedisConnection) -> RespSerializable:
        name = self._argv[2]
        value = connection.server.get_config_param(name)
        return RespArray([RespBulkString(name), RespBulkString(value)])


class DiscardCommand(RedisCommand):
    @override
    def _should_be_queued(self, connection: RedisConnection) -> bool:
        return False    # DISCARD should never be queued.

    async def _execute(self, connection: RedisConnection) -> RespSerializable:
        if not connection.transaction.discard():
            return RespSimpleError("ERR DISCARD without MULTI")
        return RespSimpleString("OK")


class EchoCommand(RedisCommand):
    async def _execute(self, connection: RedisConnection) -> RespSerializable:
        return RespBulkString(self._argv[1])


class ExecCommand(RedisCommand):
    @override
    def _should_be_queued(self, connection: RedisConnection) -> bool:
        return False    # EXEC should never be queued.

    async def _execute(self, connection: RedisConnection) -> RespSerializable:
        response = await connection.transaction.exec()
        if response is None:
            return RespSimpleError("ERR EXEC without MULTI")
        return response


class GetCommand(RedisCommand):
    async def _execute(self, connection: RedisConnection) -> RespSerializable:
        value = connection.server.database.get(key=self._argv[1])
        if value is None or type(value) is str:
            return RespBulkString(value)
        return RespSimpleError("WRONGTYPE Operation against a key holding the wrong kind of value")


class IncrCommand(RedisCommand):
    async def _execute(self, connection: RedisConnection) -> RespSerializable:
        value = connection.server.database.increment(key=self._argv[1])
        if value is not None:
            return RespInteger(value)
        return RespSimpleError("ERR value is not an integer or out of range")


class InfoCommand(RedisCommand):
    async def _execute(self, connection: RedisConnection) -> RespSerializable:
        server = connection.server
        information = (
            f"role:{server.role}\n"
            f"master_replid:{server.master_replid}\n"
            f"master_repl_offset:{server.master_repl_offset}"
        )
        return RespBulkString(information)


class KeysCommand(RedisCommand):
    async def _execute(self, connection: RedisConnection) -> RespSerializable:
        keys = connection.server.database.keys()
        return RespArray([RespBulkString(key) for key in keys])


class MultiCommand(RedisCommand):
    @override
    def _should_be_queued(self, connection: RedisConnection) -> bool:
        return False    # MULTI should never be queued.

    async def _execute(self, connection: RedisConnection) -> RespSerializable:
        if not connection.transaction.activate():
            return RespSimpleError("ERR MULTI calls can not be nested")
        return RespSimpleString("OK")


class PingCommand(RedisCommand):
    async def _execute(self, connection: RedisConnection) -> RespSerializable:
        return RespSimpleString("PONG")


class PsyncCommand(RedisCommand):
    async def _execute(self, connection: RedisConnection) -> RespSerializable:
        server = connection.server
        server.mark_as_replica(connection)
        return RespSimpleString(f"+FULLRESYNC {server.master_replid} 0")


class ReplconfCommand(RedisCommand):
    @override
    def _has_response(self, connection: RedisConnection) -> bool:
        return self._argv[1] != "ACK"

    async def _execute(self, connection: RedisConnection) -> RespSerializable:
        server = connection.server
        match self._argv[1]:
            case "GETACK":
                return ReplconfCommand(["REPLCONF", "ACK", str(server.master_repl_offset)])
            case "ACK":
                connection.ack_offset += int(self._argv[2])
        return RespSimpleString("OK")


class SetCommand(RedisCommand):
    @override
    def _should_be_propogated(self) -> bool:
        return True

    async def _execute(self, connection: RedisConnection) -> RespSerializable:
        key, value = self._argv[1], self._argv[2]
        database = connection.server.database
        if len(self._argv) == 3:
            database.set(key, value)
        else:
            expire_time = float(self._argv[-1])
            database.set(key, value, expire_time=expire_time)
        return RespSimpleString("OK")


class TypeCommand(RedisCommand):
    async def _execute(self, connection: RedisConnection) -> RespSerializable:
        value = connection.server.database.get(key=self._argv[1])
        if value is None:
            type_str = "none"
        elif type(value) is str:
            type_str = "string"
        else:
            type_str = "stream"
        return RespSimpleString(type_str)


class WaitCommand(RedisCommand):
    async def _execute(self, connection: RedisConnection) -> RespSerializable:
        server = connection.server
        propogate_offset = connection.propogate_offset

        if propogate_offset > 0:
            getack_command = ReplconfCommand(["REPLCONF", "GETACK", "*"])
            await connection.server.send_command_to_replicas(getack_command)

        required_num_replicas = int(self._argv[1])
        timeout_timestamp = get_current_timestamp() + int(self._argv[2])

        num_acked_replicas = server.get_num_acked_replicas(propogate_offset)
        while get_current_timestamp() < timeout_timestamp:
            if num_acked_replicas >= required_num_replicas:
                break
            await asyncio.sleep(0)
            num_acked_replicas = server.get_num_acked_replicas(
                propogate_offset)

        return RespInteger(num_acked_replicas)


class XaddCommand(RedisCommand):
    async def _execute(self, connection: RedisConnection) -> RespSerializable:
        database = connection.server.database

        stream_key = self._argv[1]
        if (stream := database.get(stream_key)) is None:
            stream = RedisStream()
            database.set(stream_key, stream)
        if type(stream) is not RedisStream:
            return RespSimpleError(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )

        entry_id = stream.string_to_entry_id(self._argv[2])
        kv_pairs = {k: v for k, v in itertools.batched(self._argv[3:], 2)}

        if stream.xadd(entry_id, kv_pairs):
            return RespBulkString(str(entry_id))
        elif entry_id == RedisStreamEntryId(0, 0):
            return RespSimpleError("ERR The ID specified in XADD must be greater than 0-0")
        else:
            return RespSimpleError(
                "ERR The ID specified in XADD is equal or smaller than the target stream top item"
            )


class XrangeCommand(RedisCommand):
    async def _execute(self, connection: RedisConnection) -> RespSerializable:
        stream_key = self._argv[1]
        stream = connection.server.database.get(stream_key)
        if stream is None:
            return RespArray([])
        elif type(stream) is not RedisStream:
            return RespSimpleError(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )

        min_id_str = self._argv[2]
        min_id = None if min_id_str == "-" else stream.string_to_entry_id(
            min_id_str, "min")
        max_id_str = self._argv[3]
        max_id = None if max_id_str == "+" else stream.string_to_entry_id(
            max_id_str, "max")
        return RespArray(stream.xrange(min_id, max_id))


class XreadCommand(RedisCommand):
    async def _execute(self, connection: RedisConnection) -> RespSerializable:
        i = self._argv.index("streams") + 1
        num_streams = (len(self._argv) - i) // 2
        stream_keys = self._argv[i:i+num_streams]
        streams = [connection.server.database.get(
            stream_key) for stream_key in stream_keys]
        start_ids = []
        for stream, start_id_str in zip(streams, self._argv[i+num_streams:]):
            if start_id_str == "$":
                start_id = stream.most_recent_entry_id()
            else:
                start_id = stream.string_to_entry_id(start_id_str)
            start_ids.append(start_id)

        if self._argv[1] == "block":
            block_time = int(self._argv[2])
            if block_time == 0:
                block_timestamp = float('inf')
            else:
                block_timestamp = get_current_timestamp() + block_time
        else:
            block_timestamp = 0

        while True:
            array = []
            has_data = False
            for stream, stream_key, start_id in zip(streams, stream_keys, start_ids):
                if entries := stream.xread(start_id):
                    has_data = True
                array.append(
                    RespArray([RespBulkString(stream_key), RespArray(entries)]))

            if has_data:
                return RespArray(array)
            if get_current_timestamp() >= block_timestamp:
                return RespBulkString(None)

            await asyncio.sleep(0)


def argv_to_command(argv: list[str]) -> RedisCommand:
    match command_name := argv[0].upper():
        case "CONFIG":
            return ConfigCommand(argv)
        case "DISCARD":
            return DiscardCommand(argv)
        case "ECHO":
            return EchoCommand(argv)
        case "EXEC":
            return ExecCommand(argv)
        case "GET":
            return GetCommand(argv)
        case "INCR":
            return IncrCommand(argv)
        case "INFO":
            return InfoCommand(argv)
        case "KEYS":
            return KeysCommand(argv)
        case "MULTI":
            return MultiCommand(argv)
        case "PING":
            return PingCommand(argv)
        case "PSYNC":
            return PsyncCommand(argv)
        case "REPLCONF":
            return ReplconfCommand(argv)
        case "SET":
            return SetCommand(argv)
        case "TYPE":
            return TypeCommand(argv)
        case "WAIT":
            return WaitCommand(argv)
        case "XADD":
            return XaddCommand(argv)
        case "XRANGE":
            return XrangeCommand(argv)
        case "XREAD":
            return XreadCommand(argv)
        case _:
            raise ValueError(f"Unknown command: {command_name}")
