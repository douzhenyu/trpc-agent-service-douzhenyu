"""Governance domain: data classification, DLP, secret detection and Policy Bundles.

Structured governance rules are compiled into versioned Policy Bundles, signed
with HMAC-SHA256 and evaluated by the local OPA policy and the tRPC-Agent
execution pipeline. Decisions are allow, deny, mask or needs-approval; DLP may
only raise a data classification, and detected secrets block the request
outright. RESTRICTED data never enters external models.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import re
from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class DataClassification(StrEnum):
    PUBLIC = "PUBLIC"
    INTERNAL = "INTERNAL"
    CONFIDENTIAL = "CONFIDENTIAL"
    RESTRICTED = "RESTRICTED"

    @property
    def rank(self) -> int:
        return CLASSIFICATION_ORDER.index(self.value)


CLASSIFICATION_ORDER: tuple[str, ...] = (
    "PUBLIC",
    "INTERNAL",
    "CONFIDENTIAL",
    "RESTRICTED",
)


def highest_classification(
    *classifications: DataClassification | str | None,
) -> DataClassification:
    """Aggregate classifications to the highest level present (DLP may only raise)."""

    highest = DataClassification.PUBLIC
    for candidate in classifications:
        if candidate is None:
            continue
        value = (
            candidate
            if isinstance(candidate, DataClassification)
            else DataClassification(str(candidate))
        )
        if value.rank > highest.rank:
            highest = value
    return highest


@dataclass(frozen=True)
class DetectionRule:
    name: str
    pattern: re.Pattern[str]
    classification: DataClassification | None = None


SECRET_DETECTION_RULES: tuple[DetectionRule, ...] = (
    DetectionRule("private-key-block", re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----")),
    DetectionRule("aws-access-key", re.compile(r"\bAKIA[0-9A-Z]{16}\b")),
    DetectionRule("assigned-api-key", re.compile(r"\b(?:sk|rk|pk)-[A-Za-z0-9_-]{20,}\b")),
    DetectionRule(
        "credential-assignment",
        re.compile(r"(?i)\b(api[_-]?key|secret|password|token)\s*[=:]\s*['\"][^'\"]{8,}['\"]"),
    ),
)

DLP_RULES: tuple[DetectionRule, ...] = (
    DetectionRule(
        "mainland-id-number",
        re.compile(
            r"\b[1-9]\d{5}(?:19|20)\d{2}(?:0[1-9]|1[0-2])(?:0[1-9]|[12]\d|3[01])\d{3}[\dXx]\b"
        ),
        DataClassification.CONFIDENTIAL,
    ),
    DetectionRule(
        "payment-card-number",
        re.compile(r"\b(?:\d[ -]*?){13,19}\b"),
        DataClassification.CONFIDENTIAL,
    ),
    DetectionRule(
        "mainland-phone-number",
        re.compile(r"\b1[3-9]\d{9}\b"),
        DataClassification.INTERNAL,
    ),
)


@dataclass(frozen=True)
class ContentScanResult:
    """DLP and secret detection outcome for the outbound message payload."""

    detected_secrets: tuple[str, ...]
    detected_classification: DataClassification

    @property
    def blocked(self) -> bool:
        return bool(self.detected_secrets)


def scan_messages(messages: Sequence[dict[str, Any]]) -> ContentScanResult:
    """Scan outbound messages for secrets and DLP-raising patterns."""

    detected_secrets: list[str] = []
    detected = DataClassification.PUBLIC
    for message in messages:
        # Serialize the entire payload: tool-call arguments and other
        # structured fields are screened exactly like free text.
        content = json.dumps(message, ensure_ascii=False, default=str)
        for rule in SECRET_DETECTION_RULES:
            if rule.pattern.search(content):
                detected_secrets.append(rule.name)
        for rule in DLP_RULES:
            if rule.classification is not None and rule.pattern.search(content):
                detected = highest_classification(detected, rule.classification)
    return ContentScanResult(
        detected_secrets=tuple(detected_secrets), detected_classification=detected
    )


class MaskPattern(BaseModel):
    """A DLP masking rewrite applied before content leaves the platform."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    name: str = Field(min_length=1, max_length=64)
    regex: str = Field(min_length=1, max_length=512)
    replacement: str = Field(min_length=0, max_length=128)


class GovernanceRules(BaseModel):
    """Structured governance rules compiled into a signed Policy Bundle."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    max_outbound_classification: DataClassification = DataClassification.CONFIDENTIAL
    allow_restricted_to_private_endpoints: bool = False
    require_approval_above: DataClassification | None = None
    secret_detection_enabled: bool = True
    mask_patterns: tuple[MaskPattern, ...] = ()


class DecisionType(StrEnum):
    ALLOW = "ALLOW"
    DENY = "DENY"
    MASK = "MASK"
    NEEDS_APPROVAL = "NEEDS_APPROVAL"


@dataclass(frozen=True)
class GovernanceDecision:
    """The governance verdict for one outbound request."""

    decision: DecisionType
    effective_classification: DataClassification
    reason: str
    masked_messages: list[dict[str, Any]] | None = None


def evaluate_outbound(
    rules: GovernanceRules,
    messages: Sequence[dict[str, Any]],
    *,
    declared_classification: DataClassification,
    target_is_private_endpoint: bool,
) -> GovernanceDecision:
    """Decide allow/deny/mask/needs-approval for one outbound model request."""

    scan = scan_messages(messages)
    effective = highest_classification(declared_classification, scan.detected_classification)
    if rules.secret_detection_enabled and scan.blocked:
        return GovernanceDecision(
            decision=DecisionType.DENY,
            effective_classification=effective,
            reason=f"secret detected: {', '.join(scan.detected_secrets)}",
        )
    if effective == DataClassification.RESTRICTED:
        # RESTRICTED has its dedicated gate: it never reaches external
        # models, and the private-endpoint exception is explicit.
        if not (rules.allow_restricted_to_private_endpoints and target_is_private_endpoint):
            return GovernanceDecision(
                decision=DecisionType.DENY,
                effective_classification=effective,
                reason="restricted data cannot enter external models",
            )
    elif effective.rank > rules.max_outbound_classification.rank:
        return GovernanceDecision(
            decision=DecisionType.DENY,
            effective_classification=effective,
            reason=f"{effective.value} exceeds the policy ceiling "
            f"{rules.max_outbound_classification.value}",
        )
    if (
        rules.require_approval_above is not None
        and effective.rank > DataClassification(rules.require_approval_above.value).rank
    ):
        return GovernanceDecision(
            decision=DecisionType.NEEDS_APPROVAL,
            effective_classification=effective,
            reason=f"{effective.value} exceeds the approval-free ceiling",
        )
    masked = _apply_masks(rules, messages)
    if masked is not None:
        return GovernanceDecision(
            decision=DecisionType.MASK,
            effective_classification=effective,
            reason="mask patterns applied",
            masked_messages=masked,
        )
    return GovernanceDecision(
        decision=DecisionType.ALLOW,
        effective_classification=effective,
        reason="within policy",
    )


def _apply_masks(
    rules: GovernanceRules, messages: Sequence[dict[str, Any]]
) -> list[dict[str, Any]] | None:
    if not rules.mask_patterns:
        return None
    compiled = [(pattern, re.compile(pattern.regex)) for pattern in rules.mask_patterns]
    masked: list[dict[str, Any]] | None = None
    for index, message in enumerate(messages):
        content = str(message.get("content", ""))
        rewritten = content
        for pattern, regex in compiled:
            rewritten = regex.sub(pattern.replacement, rewritten)
        if rewritten != content:
            if masked is None:
                masked = [dict(item) for item in messages]
            masked[index]["content"] = rewritten
    return masked


def compile_bundle(rules: GovernanceRules, version: int) -> dict[str, Any]:
    """Compile structured rules into the OPA data document for one bundle."""

    return {
        "version": version,
        "governance": {
            "max_outbound_classification": rules.max_outbound_classification.value,
            "allow_restricted_to_private_endpoints": rules.allow_restricted_to_private_endpoints,
            "require_approval_above": (
                rules.require_approval_above.value
                if rules.require_approval_above is not None
                else None
            ),
            "secret_detection_enabled": rules.secret_detection_enabled,
            "mask_patterns": [pattern.model_dump() for pattern in rules.mask_patterns],
        },
    }


def sign_bundle(rules: GovernanceRules, version: int, signing_key: str) -> str:
    """HMAC-SHA256 signature over the rules, the version and the compiled doc."""

    if not signing_key:
        raise ValueError("policy signing key is required")
    canonical = json.dumps(
        {
            "rules": rules.model_dump(mode="json"),
            "version": version,
            "bundle": compile_bundle(rules, version),
        },
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hmac.new(signing_key.encode(), canonical.encode(), hashlib.sha256).hexdigest()


def verify_bundle(rules: GovernanceRules, version: int, signature: str, signing_key: str) -> bool:
    """Verify a bundle signature; tampering with rules or version breaks it."""

    expected = sign_bundle(rules, version, signing_key)
    return hmac.compare_digest(expected, signature)


def canary_bucket(decision_key: str) -> int:
    """Deterministic 0-99 bucket used for policy canary assignment."""

    digest = hashlib.sha256(decision_key.encode()).digest()
    return int.from_bytes(digest[:8], "big") % 100
