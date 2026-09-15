"""Replay is the ingestion implementation; downstream consumes Message, not files."""
from pathlib import Path
from schema import Message, read_jsonl


def replay(path: Path, channel_id: str | None = None) -> list[Message]:
    messages = [Message.model_validate(row) for row in read_jsonl(path)]
    if channel_id is not None:
        messages = [m for m in messages if m.channel_id == channel_id]
    return sorted(messages, key=lambda m: (m.timestamp, m.channel_id, m.message_id))


class MessageIndex:
    def __init__(self):
        self.seen: dict[tuple[str, str], Message] = {}

    def accept(self, message: Message) -> str:
        """Record even failed extractions so transport redelivery cannot trigger a retry."""
        old = self.seen.get(message.key())
        if old is not None:
            return 'duplicate' if old == message else 'unsupported_edit'
        self.seen[message.key()] = message
        return 'new'

    def context(self, message: Message, max_ancestors: int = 3) -> list[Message]:
        chain, visited = [], {message.key()}
        target = message.reply_to
        for _ in range(max_ancestors):
            if target is None or target.key() in visited or target.key() not in self.seen:
                break
            parent = self.seen[target.key()]
            if parent.timestamp > message.timestamp:
                break
            chain.append(parent)
            visited.add(parent.key())
            target = parent.reply_to
        if message.forwarded_origin:
            original = self.seen.get(message.forwarded_origin.key())
            if original and original.key() not in visited and original.timestamp <= message.timestamp:
                chain.append(original)
        return chain
