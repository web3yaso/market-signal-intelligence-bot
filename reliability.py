"""Shared transport-failure classification for retry and recovery."""


def is_transient(reason: str | None) -> bool:
    if reason in {"timeout_or_network_error", "http_429"}:
        return True
    if reason and reason.startswith("http_"):
        code = reason.removeprefix("http_")
        return code.isdigit() and 500 <= int(code) <= 599
    return False
