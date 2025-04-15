import abc
from typing import Optional


class RespSerializable(abc.ABC):
    @abc.abstractmethod
    def serialize(self) -> bytes:
        raise NotImplementedError


class RespBulkString(RespSerializable):
    def __init__(self, string: Optional[str]) -> None:
        self.string = string

    def serialize(self) -> bytes:
        if self.string is None:
            return "$-1\r\n".encode()
        return f"${len(self.string)}\r\n{self.string}\r\n".encode()


class RespSimpleString(RespSerializable):
    def __init__(self, string: str) -> None:
        self.string = string

    def serialize(self) -> bytes:
        return f"+{self.string}\r\n".encode()
