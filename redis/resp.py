from __future__ import annotations
import abc
from typing import Optional


class RespSerializable(abc.ABC):
    @abc.abstractmethod
    def serialize(self) -> bytes:
        """Serialize the object under Redis serialization protocol (RESP)."""
        raise NotImplementedError


class RespArray(RespSerializable):
    def __init__(self, elements: Optional[list[RespSerializable]]) -> None:
        self._elements = elements

    def serialize(self) -> bytes:
        if self._elements is None:
            return "*-1\r\n".encode()
        encoded_length = f"*{len(self._elements)}\r\n".encode()
        encoded_elements = b"".join(e.serialize() for e in self._elements)
        return encoded_length + encoded_elements


class RespBulkString(RespSerializable):
    def __init__(self, string: Optional[str]) -> None:
        self._string = string

    def serialize(self) -> bytes:
        if self._string is None:
            return "$-1\r\n".encode()
        return f"${len(self._string)}\r\n{self._string}\r\n".encode()


class RespInteger(RespSerializable):
    def __init__(self, integer: int) -> None:
        self._integer = integer

    def serialize(self) -> bytes:
        return f":{self._integer}\r\n".encode()


class RespSimpleError(RespSerializable):
    def __init__(self, error_message: str) -> None:
        self._error_message = error_message

    def serialize(self) -> bytes:
        return f"-{self._error_message}\r\n".encode()


class RespSimpleString(RespSerializable):
    def __init__(self, string: str) -> None:
        self._string = string

    def serialize(self) -> bytes:
        return f"+{self._string}\r\n".encode()
