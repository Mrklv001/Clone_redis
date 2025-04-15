import argparse
import asyncio

from redis import RedisServer


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("--port", type=int, default=6379)
    parser.add_argument("--replicaof", type=str, default=None)

    parser.add_argument("--dir", type=str, default=None)
    parser.add_argument("--dbfilename", type=str, default=None)

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    address = "localhost", args.port
    if args.replicaof is not None:
        master_host, master_port = args.replicaof.split()
        master_address = master_host, int(master_port)
        server = RedisServer(address, master_address)
    else:
        server = RedisServer(address, dir=args.dir, dbfilename=args.dbfilename)

    asyncio.run(server.start())


if __name__ == "__main__":
    main()
