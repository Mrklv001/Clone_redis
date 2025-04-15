from __future__ import annotations
import asyncio
import os
from typing import TYPE_CHECKING, Literal, Optional

from .connection import ConnectionType, RedisConnection
from .database import RedisDatabase, RedisDatabaseLoader

if TYPE_CHECKING:
    from .commands import RedisCommand


Address = tuple[str, int]


class RedisServer:
    def __init__(
        self, address: Address, master_address: Optional[Address] = None, **kwargs
    ) -> None:
        self._address = address
        self._master_address = master_address

        self._master: Optional[RedisConnection] = None
        self._replicas: set[RedisConnection] = set()

        self._master_repl_offset = 0

        self._config_params: dict[str, Optional[str]] = kwargs
        self._database = self._try_load_database()

    async def start(self) -> None:
        """Start running the server."""
        server = await asyncio.start_server(self._process_client, *self._address, reuse_port=True)

        if self._master_address is not None:
            master_reader, master_writer = await asyncio.open_connection(*self._master_address)
            self._master = RedisConnection(
                master_reader, master_writer, server=self)

        async with server, asyncio.TaskGroup() as tg:
            tg.create_task(server.serve_forever())
            if self._master is not None:
                tg.create_task(self._process_connection(self._master))

    def get_config_param(self, name: str) -> Optional[str]:
        """Get the server's configuration parameter."""
        return self._config_params.get(name)

    def get_connection_type(self, connection: RedisConnection) -> ConnectionType:
        """Get a connection's type (client, master or replica)."""
        if connection is self._master:
            return ConnectionType.MASTER
        elif connection in self._replicas:
            return ConnectionType.REPLICA
        return ConnectionType.CLIENT

    def mark_as_replica(self, connection: RedisConnection) -> None:
        """Mark a connection as a replica server."""
        self._replicas.add(connection)

    async def send_command_to_replicas(self, command: RedisCommand) -> None:
        """Send a command to the replicas."""
        if not self._replicas:
            return
        data = command.serialize()
        for replica in self._replicas:
            await replica.send(data)

    def get_num_acked_replicas(self, target_offset: int) -> int:
        """Get the number of acknowledged replicas."""
        return sum(replica.ack_offset >= target_offset for replica in self._replicas)

    async def _process_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        """
        Process a client connection. This method is passed as a parameter of
        `asyncio.start_server()`, and is called whenever a new client connection
        is established. The `reader` and `writer` are used to communicate with
        the connection.
        """
        await self._process_connection(RedisConnection(reader, writer, server=self))

    async def _process_connection(self, connection: RedisConnection) -> None:
        """Process a connection."""
        await connection.process()
        self._replicas.discard(connection)
        await connection.close()

    def _try_load_database(self) -> RedisDatabase:
        """
        Try to load the database from an existing RDB file. If the operation
        fails, the server starts with an empty database.
        """
        try:
            rdb_filepath = os.path.join(
                self._config_params["dir"], self._config_params["dbfilename"]
            )
            return RedisDatabaseLoader().load(rdb_filepath)
        except:
            return RedisDatabase()

    @property
    def address(self) -> Address:
        """The server's address"""
        return self._address

    @property
    def database(self) -> RedisDatabase:
        """The database owned by this server."""
        return self._database

    @property
    def role(self) -> Literal["master", "slave"]:
        """
        The server's role. Value is "master" if the server is replica of no
        one, or "slave" if the server is a replica of some master server.
        """
        return "master" if self._master_address is None else "slave"

    @property
    def master_replid(self) -> str:
        """The replication ID of the server."""
        return "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"

    @property
    def master_repl_offset(self) -> int:
        """The server's current replication offset"""
        return self._master_repl_offset

    @master_repl_offset.setter
    def master_repl_offset(self, new_offset: int) -> None:
        self._master_repl_offset = new_offset
