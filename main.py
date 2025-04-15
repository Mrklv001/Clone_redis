import asyncio
from typing import Optional

from .resp2 import RespBulkString, RespSimpleString


async def recv_argv(reader: asyncio.StreamReader) -> Optional[list[str]]:
    try:
        argc = int((await reader.readuntil(b"\r\n"))[1:-2].decode())
        argv = []
        for _ in range(argc):
            argsize = int((await reader.readuntil(b"\r\n"))[1:-2].decode())
            argv.append((await reader.read(argsize+2))[:-2].decode())
        return argv
    except asyncio.IncompleteReadError:
        return None


database: dict[str, str] = {}


async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    while True:
        argv = await recv_argv(reader)
        if argv is None:
            break
        print(argv)

        match command_name := argv[0].upper():
            case "ECHO":
                response = RespBulkString(argv[1])
            case "GET":
                response = RespBulkString(database[argv[1]])
            case "PING":
                response = RespSimpleString("PONG")
            case "SET":
                database[argv[1]] = argv[2]
                response = RespSimpleString("OK")
            case _:
                raise ValueError(f"Unknown command: {command_name}")

        writer.write(response.serialize())
        await writer.drain()

    writer.close()
    await writer.wait_closed()


async def main() -> None:
    server = await asyncio.start_server(handle_client, "localhost", 6379, reuse_port=True)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
