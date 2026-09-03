"""Stable, locatable validation for mutable Agent Draft configuration."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from typing import Any

from trpc_service.admin_api.schemas import DraftIssueCode, DraftValidationIssue

_RESOURCE_NAME = re.compile(r"^[a-z0-9][a-z0-9._-]{0,127}$")


def _duplicate_issues(
    values: Sequence[str], *, code: DraftIssueCode, collection_path: str, noun: str
) -> list[DraftValidationIssue]:
    first_seen: dict[str, int] = {}
    issues: list[DraftValidationIssue] = []
    for index, value in enumerate(values):
        if value in first_seen:
            original = f"{collection_path}/{first_seen[value]}"
            issues.append(
                DraftValidationIssue(
                    code=code,
                    path=f"{collection_path}/{index}",
                    message=f"{noun} duplicates {original}.",
                )
            )
        else:
            first_seen[value] = index
    return issues


def validate_draft_configuration(draft: Mapping[str, Any]) -> list[DraftValidationIssue]:
    issues: list[DraftValidationIssue] = []
    if not str(draft["instructions"]).strip():
        issues.append(
            DraftValidationIssue(
                code="DRAFT_INSTRUCTIONS_REQUIRED",
                path="/instructions",
                message="Instructions must not be blank.",
            )
        )
    if not _RESOURCE_NAME.fullmatch(str(draft["model_alias"])):
        issues.append(
            DraftValidationIssue(
                code="DRAFT_MODEL_ALIAS_INVALID",
                path="/model_alias",
                message="Model alias must be a stable resource name.",
            )
        )
    issues.extend(
        _duplicate_issues(
            draft["tool_aliases"],
            code="DRAFT_DUPLICATE_TOOL_ALIAS",
            collection_path="/tool_aliases",
            noun="Tool alias",
        )
    )
    issues.extend(
        _duplicate_issues(
            draft["knowledge_refs"],
            code="DRAFT_DUPLICATE_KNOWLEDGE_REF",
            collection_path="/knowledge_refs",
            noun="Knowledge reference",
        )
    )
    policy = draft["governance_policy_ref"]
    if policy is not None and not _RESOURCE_NAME.fullmatch(str(policy)):
        issues.append(
            DraftValidationIssue(
                code="DRAFT_GOVERNANCE_POLICY_REF_INVALID",
                path="/governance_policy_ref",
                message="Governance policy reference must be a stable resource name.",
            )
        )
    return issues
