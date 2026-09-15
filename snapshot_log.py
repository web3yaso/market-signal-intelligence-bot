"""Append-only event deltas; materialize independent snapshots only when read."""

from __future__ import annotations

import copy
from collections.abc import Iterator, Sequence
from pathlib import Path
from schema import read_jsonl


class SnapshotLog(Sequence):
    def __init__(self, entries: list[dict] | None = None):
        self.entries = entries or []

    def append(self, metadata: dict, changes: dict) -> None:
        self.entries.append(
            {
                **copy.deepcopy(metadata),
                "format": "delta-v1",
                "event_changes": copy.deepcopy(changes),
            }
        )

    def __len__(self) -> int:
        return len(self.entries)

    def __iter__(self) -> Iterator[dict]:
        events = {}
        for entry in self.entries:
            events.update(entry["event_changes"])
            yield self._materialize(entry, events)

    def __getitem__(self, index):
        if isinstance(index, slice):
            return [self[i] for i in range(*index.indices(len(self)))]
        if index < 0:
            index += len(self)
        if not 0 <= index < len(self):
            raise IndexError(index)
        events = {}
        for entry in self.entries[: index + 1]:
            events.update(entry["event_changes"])
        return self._materialize(self.entries[index], events)

    def __eq__(self, other):
        return list(self) == list(other)

    @staticmethod
    def _materialize(entry: dict, events: dict) -> dict:
        metadata = {
            k: v for k, v in entry.items() if k not in ("format", "event_changes")
        }
        return copy.deepcopy({**metadata, "events": list(events.values())})


def load_snapshots(path: Path):
    rows = read_jsonl(path)
    return SnapshotLog(rows) if rows and rows[0].get("format") == "delta-v1" else rows
