"""Bounded text-only redaction. See the task plan for the supported grammar.

This module has no transport knowledge. Call finish only on a verified successful
completion; abort on truncated streams, invalid events, errors or cancellation.
"""

import re
from collections.abc import Iterator


_EMAIL = r"[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)*\.[A-Za-z]{2,}"
_CARD = r"[0-9](?:[ -]?[0-9]){12,18}"
_SSN = r"[0-9]{3}-[0-9]{2}-[0-9]{4}"
# Zero-width matching preserves overlaps: consuming a card first would hide the
# start of an intersecting email and let its domain escape ordinary re.sub.
_SENSITIVE = re.compile(f"(?=({_EMAIL}|{_CARD}|{_SSN}))")

# These deliberately overapproximate valid prefixes. A word may become an email
# local part; a trailing space may separate the next card digit. Both must wait.
_EMAIL_SUFFIX = re.compile(r"[A-Za-z0-9._%+-]+(?:@[A-Za-z0-9.-]*)?\Z")
_NUMERIC_SUFFIX = re.compile(r"[0-9](?:[ -]?[0-9])*[ -]?\Z")


def sensitive_spans(text: str) -> Iterator[tuple[int, int]]:
    """Merge intersecting matches so every recognized character is protected."""
    pending: tuple[int, int] | None = None
    for match in _SENSITIVE.finditer(text):
        start, end = match.span(1)
        if pending is not None and start < pending[1]:
            pending = (pending[0], max(pending[1], end))
        else:
            if pending is not None:
                yield pending
            pending = (start, end)
    if pending is not None:
        yield pending


def redact_complete(text: str) -> str:
    parts: list[str] = []
    offset = 0
    for start, end in sensitive_spans(text):
        parts.extend((text[offset:start], "[REDACTED]"))
        offset = end
    parts.append(text[offset:])
    return "".join(parts)


class RedactionError(ValueError):
    """Safe, fixed-message failure; never includes the pending text."""


class StreamingRedactor:
    """Release confirmed-safe prefixes while retaining bounded ambiguity.

    Limits count Unicode characters, not UTF-8 bytes. Input and returned output
    are owned by the caller; only unresolved text persists between feed calls.
    """

    def __init__(self, max_pending_chars: int = 256) -> None:
        if type(max_pending_chars) is not int or max_pending_chars <= 0:
            raise ValueError("max_pending_chars must be a positive integer")
        self.max_pending_chars = max_pending_chars
        self._pending = ""
        self._closed = False
        self.peak_pending_chars = 0

    @property
    def pending_chars(self) -> int:
        return len(self._pending)

    def _check_open(self) -> None:
        if self._closed:
            raise RedactionError("Redactor is closed")

    def abort(self) -> None:
        """Discard unresolved text and permanently close this stream."""
        self._pending = ""
        self._closed = True

    def feed(self, text: str) -> str:
        self._check_open()
        output: list[str] = []
        # Inspect one character at a time so a large delta cannot bypass the cap.
        # Work depends on the bounded candidate, never on prior stream length.
        for char in text:
            # Even a delimiter must not temporarily grow state beyond the cap.
            # A full candidate therefore aborts rather than gambling on its tail.
            if len(self._pending) == self.max_pending_chars:
                self.abort()
                raise RedactionError("Pending text limit exceeded")
            self._pending += char
            self.peak_pending_chars = max(self.peak_pending_chars, len(self._pending))

            cutoff = len(self._pending)
            for pattern in (_EMAIL_SUFFIX, _NUMERIC_SUFFIX):
                candidate = pattern.search(self._pending)
                if candidate is not None:
                    cutoff = min(cutoff, candidate.start())
                    if pattern is _NUMERIC_SUFFIX:
                        digits = sum("0" <= item <= "9" for item in candidate.group())
                        if digits > 19:
                            # Releasing a prefix of an overlong numeric run could
                            # conceal a supported suffix in the next text delta.
                            self.abort()
                            raise RedactionError("Numeric candidate limit exceeded")

            # Nothing can be released while the entire window is ambiguous.
            # Avoid repeatedly scanning long local parts for complete matches.
            if cutoff == 0:
                continue
            # A recognized value can overlap the earliest ambiguous suffix.
            # Retain the whole value so a flush cannot expose its leading part.
            for start, end in sensitive_spans(self._pending):
                if start < cutoff < end:
                    cutoff = start
                    break
            if cutoff:
                output.append(redact_complete(self._pending[:cutoff]))
                self._pending = self._pending[cutoff:]
        return "".join(output)

    def finish(self) -> str:
        """Finalize only when no more provider text can arrive successfully."""
        self._check_open()
        # Verified EOS removes the possibility of more text. Incomplete values
        # such as jane@exam remain unchanged under the pattern-only contract,
        # including a valid length/content_filter finish. Broken transports must
        # use abort instead so they cannot release that unresolved suffix.
        result = redact_complete(self._pending)
        self.abort()
        return result
