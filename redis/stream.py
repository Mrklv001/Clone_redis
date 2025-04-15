from __future__ import annotations
import dataclasses
import itertools
from typing import Literal, Optional

from .resp import RespArray, RespBulkString, RespSerializable
from .utils import get_current_timestamp


MAX_SEQ_NUM = 18446744073709551615


@dataclasses.dataclass(order=True, frozen=True)
class RedisStreamEntryId:
    milliseconds: int
    sequence_number: int

    def __add__(self, other: RedisStreamEntryId) -> RedisStreamEntryId:
        return RedisStreamEntryId(
            self.milliseconds + other.milliseconds,
            self.sequence_number + other.sequence_number,
        )

    def __str__(self) -> str:
        return f"{self.milliseconds}-{self.sequence_number}"


@dataclasses.dataclass
class RedisStreamEntry(RespSerializable):
    entry_id: RedisStreamEntryId
    kv_pairs: dict[str, str]

    def serialize(self) -> bytes:
        return RespArray([
            RespBulkString(str(self.entry_id)),
            RespArray([
                RespBulkString(s) for s in itertools.chain(*self.kv_pairs.items())
            ]),
        ]).serialize()


class RedisStream:
    def __init__(self) -> None:
        self._seq_lookup: dict[int, int] = {}
        self._entries: dict[RedisStreamEntryId, RedisStreamEntry] = {}

    def string_to_entry_id(
        self, string: str, seq_default: Literal["min", "max"] = "min",
    ) -> RedisStreamEntryId:
        """
        Generate an entry ID from a string. An entry ID consists of two parts:
        milliseconds and sequence number. The given string can be an asterisk
        ('*'), represent an integer, or have the following format:

            <milliseconds>-<sequence_number>

        - If the string is an asterisk, the current UNIX timestamp is used for
        milliseconds, and 0 for the sequence number.
        - If the string represents an integer, it is used for milliseconds.
        The sequence number is chosen based on the `seq_default` argument: 0
        ("min") or 18446744073709551615 ("max").
        - If only the sequence number part is an asterisk (e.g. "1-*"), Redis
        picks the last sequence number with the same milliseconds in the stream
        and increments it by 1. The sequence number defaults to 1 if
        milliseconds is 0, or 0 otherwise.
        """
        if string == "*":
            milliseconds = int(get_current_timestamp())
            return RedisStreamEntryId(milliseconds, 0)

        if "-" not in string:
            milliseconds = int(string)
            return RedisStream(milliseconds, 0 if seq_default == "min" else MAX_SEQ_NUM)

        msec_str, seqnum_str = string.split("-", maxsplit=1)
        milliseconds = int(msec_str)

        if seqnum_str != "*":
            sequence_number = int(seqnum_str)
        elif milliseconds not in self._seq_lookup:
            sequence_number = 0 if milliseconds > 0 else 1
        else:
            sequence_number = self._seq_lookup[milliseconds] + 1

        return RedisStreamEntryId(milliseconds, sequence_number)

    def xadd(self, entry_id: RedisStreamEntryId, kv_pairs: dict[str, str]) -> bool:
        """
        Add an entry to the stream. The returned boolean indicates whether the
        operation is successful. This operation will fail on two occasions:
        - The provided entry ID is "0-0", which is disallowed.
        - The provided entry ID isn't greater than the most recently added ID.
        """
        if entry_id <= self.most_recent_entry_id():
            return False
        self._entries[entry_id] = RedisStreamEntry(entry_id, kv_pairs)
        self._seq_lookup[entry_id.milliseconds] = entry_id.sequence_number
        return True

    def xrange(
        self, min_id: Optional[RedisStreamEntryId], max_id: Optional[RedisStreamEntryId],
    ) -> list[RedisStreamEntry]:
        """
        Get all the entries in the stream whose IDs are in the given range
        (inclusive).
        - If `min_id` is None, it's set to the minimum ID possible.
        - If `max_id` is None, it's set to the maximum ID possible.
        """
        if min_id is None:
            min_id = RedisStreamEntryId(0, 0)
        if max_id is None:
            max_id = self.most_recent_entry_id() + RedisStreamEntryId(0, 1)
        return [
            entry for entry_id, entry in self._entries.items() if min_id <= entry_id <= max_id
        ]

    def xread(self, start_id: RedisStreamEntryId) -> list[RedisStreamEntry]:
        return self.xrange(start_id + RedisStreamEntryId(0, 1), None)

    def most_recent_entry_id(self) -> RedisStreamEntryId:
        """Get the most recently added entry ID."""
        return next(reversed(self._entries)) if self._entries else RedisStreamEntryId(0, 0)
