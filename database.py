import dataclasses
import math
import time
from typing import Optional


@dataclasses.dataclass(frozen=True)
class RedisDatabaseValue:
    value: str
    expire_timestamp: float


def get_current_timestamp() -> float:
    return time.time_ns() / 1e6


class RedisDatabase:
    def __init__(self) -> None:
        self._database: dict[str, RedisDatabaseValue] = {}

    def get(self, key: str) -> Optional[str]:
        if (item := self._database.get(key)) is None:
            return None
        elif get_current_timestamp() >= item.expire_timestamp:
            self._database.pop(key)
            return None
        return item.value

    def set(self, key: str, value: str, *, expire_time: Optional[float] = None) -> None:
        if expire_time is None:
            expire_timestamp = math.inf
        else:
            expire_timestamp = get_current_timestamp() + expire_time
        self._database[key] = RedisDatabaseValue(value, expire_timestamp)
