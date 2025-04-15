from __future__ import annotations
import dataclasses
import io
from typing import Optional, Union

from .stream import RedisStream
from .utils import get_current_timestamp


DataStruct = Union[RedisStream, str]


@dataclasses.dataclass
class RedisDatabaseValue:
    value: DataStruct
    expire_timestamp: float = float('inf')


class RedisDatabase:
    def __init__(self):
        self._database: dict[str, RedisDatabaseValue] = {}

    def get(self, key: str) -> Optional[DataStruct]:
        """Get the value stored at a key, or None if the key has expired."""
        if (item := self._database.get(key)) is None:
            return None
        elif get_current_timestamp() >= item.expire_timestamp:
            self._database.pop(key)
            return None
        return item.value

    def set(
        self,
        key: str,
        value: DataStruct,
        *,
        expire_time: Optional[float] = None,
        expire_timestamp: Optional[float] = None,
    ) -> None:
        """
        Set the value stored at a key, with an optional expire time (relative)
        or expire timestamp (absolute), both in milliseconds.
        """
        if expire_timestamp is None:
            if expire_time is None:
                expire_timestamp = float("inf")
            else:
                expire_timestamp = get_current_timestamp() + expire_time
        self._database[key] = RedisDatabaseValue(value, expire_timestamp)

    def increment(self, key: str) -> Optional[int]:
        """
        Increment the value stored at a key, and return the incremented value.
        If the key doesn't exist, it's set to 0 before the operation. None is
        returned if the key contains a value of the wrong type or a string that
        can't be represented as integer.
        """
        if (item := self._database.get(key)) is None:
            self.set(key, value="1")
            return 1
        try:
            incremented_value = int(item.value) + 1
            item.value = str(incremented_value)
            return incremented_value
        except:
            return None

    def keys(self) -> list[str]:
        """Get a list of all keys in the database."""
        return list(self._database.keys())

    def dump(self) -> bytes:
        """Convert the database into RDB binary file format."""
        return bytes.fromhex(
            "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374"
            "696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
        )


class RdbBytesIO(io.BytesIO):
    def consume(self, expected: bytes) -> bool:
        if self.read(1) == expected:
            return True
        self.seek(-1, io.SEEK_CUR)
        return False

    def read_size(self) -> int:
        mode, remainder = divmod(int.from_bytes(self.read(1)), 64)
        match mode:
            case 0:
                return remainder
            case 1:
                return remainder * 256 + int.from_bytes(self.read(1))
            case 2:
                return int.from_bytes(self.read(4))
            case _:
                raise ValueError(
                    "Expected size encoding, got string encoding instead")

    def read_string(self) -> str:
        if self.consume(b"\xc0"):
            return str(int.from_bytes(self.read(1), byteorder="little"))
        elif self.consume(b"\xc1"):
            return str(int.from_bytes(self.read(2), byteorder="little"))
        elif self.consume(b"\xc2"):
            return str(int.from_bytes(self.read(4), byteorder="little"))
        elif self.consume(b"\xc3"):
            raise ValueError("Unexpected LZF compression")

        length = self.read_size()
        return self.read(length).decode()


class RedisDatabaseLoader:
    def load(self, rdb_filepath: str) -> RedisDatabase:
        with open(rdb_filepath, mode="rb") as f:
            self._reader = RdbBytesIO(f.read())
        self._load_header()
        self._load_metadata()
        return self._load_database()

    def _load_header(self) -> None:
        self._reader.read(9)

    def _load_metadata(self) -> None:
        while self._reader.consume(b"\xfa"):
            self._reader.read_string()
            self._reader.read_string()

    def _load_database(self) -> RedisDatabase:
        database = RedisDatabase()
        if self._reader.consume(b"\xfe"):
            self._reader.read_size()    # Database index.
            assert self._reader.consume(
                b"\xfb"), "Missing hash table size information."
            total_size = self._reader.read_size()
            self._reader.read_size()    # Number of keys with expiry.

            for _ in range(total_size):
                expire_timestamp = self._load_expire_timestamp()
                key, value = self._load_key_value_pair()
                database.set(key, value, expire_timestamp=expire_timestamp)

        return database

    def _load_expire_timestamp(self) -> float:
        if self._reader.consume(b"\xfc"):
            return int.from_bytes(self._reader.read(8), byteorder="little")
        elif self._reader.consume(b"\xfd"):
            return int.from_bytes(self._reader.read(4), byteorder="little") * 1000
        return float("inf")

    def _load_key_value_pair(self) -> tuple[str, str]:
        assert self._reader.consume(b"\x00"), "Value type should be string."
        return self._reader.read_string(), self._reader.read_string()
