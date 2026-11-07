"""Synthetic fixtures; test output prefixes as well as final replacements."""

import random

import pytest

from stream_guardrail.redactor import RedactionError, StreamingRedactor


SENSITIVE = [
    "jane@example.com", "Jane.Doe+test@Sub.Example.COM",
    "123-45-6789", "4111111111111", "4111 1111 1111 1111",
    "4111-1111-1111-1111", "1234567890123456789",
    "4111 1111-1111 1111",  # Syntactic policy, including mixed separators.
]


@pytest.mark.parametrize("value", SENSITIVE)
def test_every_split_and_character_deltas(value):
    for chunks in ([value[:i], value[i:]] for i in range(len(value) + 1)):
        redactor = StreamingRedactor()
        output = redactor.feed("Contact: ")
        assert output == "Contact: "
        for chunk in chunks:
            output += redactor.feed(chunk)
            assert output == "Contact: "  # No partial sensitive prefix is released.
        assert output + redactor.feed("!") + redactor.finish() == "Contact: [REDACTED]!"

    redactor = StreamingRedactor()
    assert all(redactor.feed(char) == "" for char in value)
    assert redactor.finish() == "[REDACTED]"


@pytest.mark.parametrize("text", [
    "Hello world! café 😀\n", "Price: 12.50; ref 1234.", "", "a@b", "123-45",
    "a.b+c%test", "Unicode: 日本語", "two  spaces\tand\r\nlines",
])
def test_safe_text_preserved(text):
    redactor = StreamingRedactor()
    assert "".join(redactor.feed(char) for char in text) + redactor.finish() == text


def test_multiple_values_punctuation_and_empty_deltas():
    redactor = StreamingRedactor()
    text = "(jane@example.com), SSN: 123-45-6789; card: 4111 1111 1111 1111."
    output = redactor.feed("")
    output += "".join(redactor.feed(char) for char in text)
    assert output + redactor.finish() == "([REDACTED]), SSN: [REDACTED]; card: [REDACTED]."


def test_safe_prefix_released_without_waiting_for_finish():
    redactor = StreamingRedactor()
    assert redactor.feed("Contact jane") == "Contact "
    assert redactor.feed("@example") == ""
    assert redactor.feed(".com for details.") == "[REDACTED] for "
    assert redactor.finish() == "details."


@pytest.mark.parametrize("value", ["x" * 33, "jane@" + "x" * 28])
def test_candidate_limit_is_terminal_and_sanitized(value):
    redactor = StreamingRedactor(max_pending_chars=32)
    assert redactor.feed(value[:32]) == ""
    with pytest.raises(RedactionError, match="^Pending text limit exceeded$"):
        redactor.feed(value[32:])
    assert redactor.pending_chars == 0
    with pytest.raises(RedactionError, match="closed"):
        redactor.finish()
    with pytest.raises(RedactionError, match="closed"):
        redactor.feed("safe")


def test_large_safe_input_does_not_require_large_pending_buffer():
    redactor = StreamingRedactor(max_pending_chars=32)
    text = "Safe text! " * 10000
    assert redactor.feed(text) + redactor.finish() == text
    assert redactor.peak_pending_chars <= 32


def test_overlong_numeric_run_fails_without_releasing_digits():
    redactor = StreamingRedactor()
    assert redactor.feed("1 " * 19) == ""
    with pytest.raises(RedactionError, match="Numeric candidate limit exceeded"):
        redactor.feed("1")
    assert redactor.pending_chars == 0


def test_finish_is_terminal():
    redactor = StreamingRedactor()
    assert redactor.feed("jane@example.com") == ""
    assert redactor.finish() == "[REDACTED]"
    with pytest.raises(RedactionError, match="closed"):
        redactor.feed("more")


@pytest.mark.parametrize("limit", [0, -1, True, 1.5, "256"])
def test_invalid_limit(limit):
    with pytest.raises(ValueError, match="positive integer"):
        StreamingRedactor(max_pending_chars=limit)


def test_abort_discards_pending_content():
    redactor = StreamingRedactor()
    assert redactor.feed("jane@exam") == ""
    redactor.abort()
    assert redactor.pending_chars == 0
    with pytest.raises(RedactionError, match="closed"):
        redactor.finish()


def test_random_partitions_preserve_order_and_match_single_delta():
    text = " / ".join(SENSITIVE) + "! Ordinary café 😀 prose."
    expected = " / ".join(["[REDACTED]"] * len(SENSITIVE)) + "! Ordinary café 😀 prose."
    randomizer = random.Random(42)
    for _ in range(100):
        redactor = StreamingRedactor()
        output = []
        offset = 0
        while offset < len(text):
            size = randomizer.randint(1, 31)
            output.append(redactor.feed(text[offset:offset + size]))
            offset += size
        assert "".join(output) + redactor.finish() == expected


def test_pending_growth_independent_of_total_stream_length():
    redactor = StreamingRedactor(max_pending_chars=32)
    for _ in range(10000):
        assert redactor.feed("Hello! ") == "Hello! "
        assert redactor.pending_chars == 0
    assert redactor.peak_pending_chars == 6
    assert redactor.finish() == ""


def test_overlapping_email_and_numeric_candidates_do_not_expose_prefix():
    value = "123-45-6789@example.com"
    redactor = StreamingRedactor()
    for char in value:
        assert redactor.feed(char) == ""
    assert redactor.feed("!") == "[REDACTED]!"
    assert redactor.finish() == ""


def test_long_bounded_local_parts_and_prose_preserve_release_boundary():
    local = "a" * 230
    redactor = StreamingRedactor()
    assert redactor.feed(local) == ""
    assert redactor.feed("@example.com! ") == "[REDACTED]! "
    prose = ("b" * 250 + "! ") * 100
    assert redactor.feed(prose) + redactor.finish() == prose


@pytest.mark.parametrize("value", [
    "4111 1111 1111 1111@example.com",
    "a@b.co@c.com",
    "a@b.co@c.com@d.net",
])
def test_overlapping_supported_values_redact_the_entire_connected_span(value):
    # Non-overlapping substitution hides the local part of the second match
    # while exposing its domain. Every character in either match must be masked.
    for split in range(len(value) + 1):
        redactor = StreamingRedactor()
        assert redactor.feed(value[:split]) == ""
        assert redactor.feed(value[split:]) == ""
        assert redactor.feed("!") + redactor.finish() == "[REDACTED]!"
    redactor = StreamingRedactor()
    for char in value:
        assert redactor.feed(char) == ""
    assert redactor.finish() == "[REDACTED]"
