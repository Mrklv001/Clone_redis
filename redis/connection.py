from __future__ import annotations
import asyncio
import enum
from typing import TYPE_CHECKING, Optional

from .commands import PingCommand, ReplconfCommand, PsyncCommand, argv_to_command
from .transaction import RedisTransaction

if TYPE_CHECKING:
    from .commands import RedisCommand
    from .server import RedisServer


ConnectionType = enum.Enum("ConnectionType", "CLIENT MASTER REPLICA")


class RedisConnection:
    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        server: RedisServer,
    ) -> None:
        self._reader = reader
        self._writer = writer
        self._server = server
        self._transaction = RedisTransaction(connection=self)

        self.propogate_offset = 0
        self.ack_offset = 0

    async def process(self) -> None:
        """Process the connection."""
        if self.type is ConnectionType.MASTER:
            await self._handshake()

        while True:
            if (command := await self._recv_command()) is None:
                return
            response = await command.execute(connection=self)
            if response is not None:
                await self.send(response.serialize())

            if type(command) is PsyncCommand:
                rdb_data = self._server.database.dump()
                await self.send(f"${len(rdb_data)}\r\n".encode() + rdb_data)

    async def send(self, data: bytes) -> None:
        """Send data to the connection."""
        self._writer.write(data)
        await self._writer.drain()

    async def close(self) -> None:
        """Close the connection."""
        self._writer.close()
        await self._writer.wait_closed()

    async def _recv_command(self) -> Optional[RedisCommand]:
        """Receive a command from the connection."""
        try:
            encoded_argc = await self._reader.readuntil(b"\r\n")
            argc = int(encoded_argc[1:-2].decode())
            argv = []
            for _ in range(argc):
                encoded_arglen = await self._reader.readuntil(b"\r\n")
                arglen = int(encoded_arglen[1:-2].decode())
                encoded_arg = await self._reader.readexactly(arglen + 2)
                argv.append(encoded_arg[:-2].decode())
            return argv_to_command(argv)
        except asyncio.IncompleteReadError:
            return None

    async def _handshake(self) -> None:
        """Handshake with the master server."""
        command = PingCommand(["PING"])
        await self.send(command.serialize())
        await self._reader.readuntil(b"\r\n")

        _, server_port = self._server.address
        command = ReplconfCommand(
            ["REPLCONF", "listening-port", str(server_port)])
        await self.send(command.serialize())
        await self._reader.readuntil(b"\r\n")

        command = ReplconfCommand(["REPLCONF", "capa", "psync2"])
        await self.send(command.serialize())
        await self._reader.readuntil(b"\r\n")

        command = PsyncCommand(["PSYNC", "?", "-1"])
        await self.send(command.serialize())
        await self._reader.readuntil(b"\r\n")

        encoded_filesize = await self._reader.readuntil(b"\r\n")
        filesize = int(encoded_filesize[1:-2].decode())
        await self._reader.readexactly(filesize)

    @property
    def server(self) -> RedisServer:
        """The server processing this connection."""
        return self._server

    @property
    def transaction(self) -> RedisTransaction:
        """The transaction owned by this connection."""
        return self._transaction

    @property
    def type(self) -> ConnectionType:
        """Type of connection (client, master, or replica)."""
        return self._server.get_connection_type(connection=self)
