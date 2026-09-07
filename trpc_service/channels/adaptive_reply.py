"""Channel-capability reply planning with bounded, Unicode-safe segments."""

from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field


class ReplyStrategy(StrEnum):
    """The visible delivery behaviour selected for one conversation."""

    MERGED_STREAM = "MERGED_STREAM"
    FINAL_ONLY = "FINAL_ONLY"


class ChannelReplyCapabilities(BaseModel):
    """The explicit limits used when adapting a reply to an IM channel."""

    model_config = ConfigDict(frozen=True)

    supports_updates: bool
    max_text_chars: int = Field(default=4096, ge=1, le=100_000)
    stream_chunk_chars: int = Field(default=256, ge=1, le=100_000)


class ReplyPlan(BaseModel):
    """A reply that can be delivered without relying on implicit channel limits."""

    model_config = ConfigDict(frozen=True)

    strategy: ReplyStrategy
    processing_notice: bool
    stream_chunks: tuple[str, ...]
    final_messages: tuple[str, ...]


def split_reply_text(content: str, *, max_chars: int) -> tuple[str, ...]:
    """Split on Unicode code points, preferring a nearby line/word boundary.

    IM APIs apply their limits to user-visible characters.  Python strings are
    Unicode code-point sequences, so this never slices a UTF-8 byte sequence.
    It also keeps provider JSON serialization separate from the message text,
    which avoids treating a reply as executable markup or a shell fragment.
    """

    clean = content.replace("\x00", "")
    if not clean:
        return ("…",)
    pieces: list[str] = []
    remaining = clean
    while len(remaining) > max_chars:
        boundary = _markdown_boundary(remaining, max_chars)
        if boundary <= 0:
            boundary = max_chars
        piece = remaining[:boundary].rstrip()
        if not piece:
            piece = remaining[:max_chars]
            boundary = max_chars
        pieces.append(piece)
        remaining = remaining[boundary:].lstrip(" \n")
    if remaining:
        pieces.append(remaining)
    return tuple(pieces)


def _markdown_boundary(value: str, maximum: int) -> int:
    """Return a visible-text boundary that does not bisect Markdown syntax.

    A reply fragment must never leave an unterminated fenced block or link in
    a provider card.  If no such boundary exists (for example an unbroken URL
    longer than the provider limit), callers should artifactize it instead of
    pretending an unsafe split is a Markdown-safe delivery.
    """

    candidates = [
        index
        for index in range(1, maximum + 1)
        if value[index - 1] in {"\n", " "} and _markdown_state(value[:index]) == (0, 0)
    ]
    if candidates:
        return candidates[-1]
    # Plain text has no Markdown constructs and can always use a hard limit.
    if _markdown_state(value[:maximum]) == (0, 0):
        return maximum
    return 0


def _markdown_state(value: str) -> tuple[int, int]:
    """Track fenced-code and link-bracket nesting for a fragment boundary."""

    fences = value.count("```") % 2
    brackets = 0
    for character in value:
        if character == "[":
            brackets += 1
        elif character == "]" and brackets:
            brackets -= 1
    return fences, brackets


def plan_reply(
    content: str,
    *,
    capabilities: ChannelReplyCapabilities,
    is_group: bool,
) -> ReplyPlan:
    """Select merged streaming only for a direct conversation with updates.

    Group conversations deliberately never expose intermediate model output:
    they receive a processing notice followed by bounded final messages.  This
    is both less noisy and prevents partial content from being amplified in a
    larger audience.
    """

    final_messages = split_reply_text(content, max_chars=capabilities.max_text_chars)
    if is_group or not capabilities.supports_updates:
        return ReplyPlan(
            strategy=ReplyStrategy.FINAL_ONLY,
            processing_notice=is_group,
            stream_chunks=(),
            final_messages=final_messages,
        )
    return ReplyPlan(
        strategy=ReplyStrategy.MERGED_STREAM,
        processing_notice=False,
        stream_chunks=split_reply_text(content, max_chars=capabilities.stream_chunk_chars),
        final_messages=final_messages,
    )
