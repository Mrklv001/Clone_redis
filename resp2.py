import abc


class RespSerializable(abc.ABC):
    @abc.abstractmethod
    def serialize(self) -> bytes:
        raise NotImplementedError


class RespBulkString(RespSerializable):
    def __init__(self, string: str) -> None:
        self.string = string

    def serialize(self) -> bytes:
        return f"${len(self.string)}\r\n{self.string}\r\n".encode()


class RespSimpleString(RespSerializable):
    def __init__(self, string: str) -> None:
        self.string = string

    def serialize(self) -> bytes:
        return f"+{self.string}\r\n".encode()
