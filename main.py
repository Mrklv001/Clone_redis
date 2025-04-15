import asyncio
import socket


async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    while True:
        data = await reader.read(8192)
        if not data:
            break

        writer.write("+PONG\r\n".encode())
        await writer.drain()

    writer.close()
    await writer.wait_closed()


async def main() -> None:
    server = await asyncio.start_server(handle_client, "localhost", 6379, reuse_port=True)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
