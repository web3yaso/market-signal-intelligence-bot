"""Replay is the ingestion implementation; downstream consumes Message, not files."""

from pathlib import Path
from schema import Message, read_jsonl
from reliability import is_transient


def replay(path: Path, channel_id: str | None = None) -> list[Message]:
    messages = [Message.model_validate(row) for row in read_jsonl(path)]
    if channel_id is not None:
        messages = [m for m in messages if m.channel_id == channel_id]
    return sorted(messages, key=lambda m: (m.timestamp, m.channel_id, m.message_id))


class MessageIndex:
    def __init__(self):
        self.seen: dict[tuple[str, str], Message] = {}
        self.states: dict[tuple[str, str], str] = {}

    def accept(self, message: Message) -> str:
        """Separate delivery identity from the latest processing outcome."""
        old = self.seen.get(message.key())
        if old is not None:
            if old != message:
                return "unsupported_edit"
            return (
                "retry" if is_transient(self.states.get(message.key())) else "duplicate"
            )
        self.seen[message.key()] = message
        self.states[message.key()] = "pending"
        return "new"

    def finish(self, message: Message, status: str, reason: str | None):
        self.states[message.key()] = reason or status

    def context(
        self, message: Message, max_ancestors: int = 3, same_sender_minutes: float = 0
    ) -> list[Message]:
        chain, visited = [], {message.key()}
        target = message.reply_to
        for _ in range(max_ancestors):
            if (
                target is None
                or target.key() in visited
                or target.key() not in self.seen
            ):
                break
            parent = self.seen[target.key()]
            if parent.timestamp > message.timestamp:
                break
            chain.append(parent)
            visited.add(parent.key())
            target = parent.reply_to
        if message.forwarded_origin:
            original = self.seen.get(message.forwarded_origin.key())
            if (
                original
                and original.key() not in visited
                and original.timestamp <= message.timestamp
            ):
                chain.append(original)
        if (
            not message.reply_to
            and not message.forwarded_origin
            and same_sender_minutes > 0
        ):
            candidates = [
                m
                for m in self.seen.values()
                if m.key() != message.key()
                and m.channel_id == message.channel_id
                and m.sender_id == message.sender_id
                and 0
                <= (message.timestamp - m.timestamp).total_seconds()
                <= same_sender_minutes * 60
            ]
            if candidates:
                chain.append(max(candidates, key=lambda m: (m.timestamp, m.message_id)))
        return chain
