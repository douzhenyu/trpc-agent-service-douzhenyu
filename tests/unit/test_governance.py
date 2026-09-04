from uuid import uuid4

import pytest
from pydantic import ValidationError

from trpc_service.governance import (
    DataClassification,
    GovernanceRules,
    MaskPattern,
    canary_bucket,
    compile_bundle,
    evaluate_outbound,
    highest_classification,
    scan_messages,
    sign_bundle,
    verify_bundle,
)


def test_highest_classification_aggregates_and_never_lowers() -> None:
    assert highest_classification() is DataClassification.PUBLIC
    assert highest_classification("INTERNAL") is DataClassification.INTERNAL
    assert highest_classification(DataClassification.PUBLIC, "RESTRICTED") is (
        DataClassification.RESTRICTED
    )
    # DLP raising is one-way: a detected level can only elevate the payload.
    assert highest_classification(DataClassification.RESTRICTED, "PUBLIC") is (
        DataClassification.RESTRICTED
    )


def test_scan_messages_detects_secrets() -> None:
    scan = scan_messages(
        [
            {"role": "user", "content": "use this key sk-abcdefghijklmnopqrst1234 please"},
            {"role": "assistant", "content": "-----BEGIN RSA PRIVATE KEY----- block"},
        ]
    )
    assert scan.blocked
    assert set(scan.detected_secrets) == {"assigned-api-key", "private-key-block"}
    assert scan.detected_classification is DataClassification.PUBLIC


def test_scan_messages_raises_classification_through_dlp() -> None:
    scan = scan_messages(
        [
            {"role": "user", "content": "身份证 11010519491231002X 转账到卡号 4111 1111 1111 1111"},
            {"role": "assistant", "content": "联系 13800138000"},
        ]
    )
    assert not scan.blocked
    # DLP may only raise: the payload lands at CONFIDENTIAL, never lower.
    assert scan.detected_classification is DataClassification.CONFIDENTIAL
    plain = scan_messages([{"role": "user", "content": "普通文本"}])
    assert plain.detected_classification is DataClassification.PUBLIC


def _rules(**overrides: object) -> GovernanceRules:
    return GovernanceRules(**overrides)


def test_evaluate_outbound_blocks_secrets_before_anything_else() -> None:
    messages = [
        {"role": "user", "content": "token = 'super-secret-value-1' and 身份证 11010519491231002X"}
    ]
    decision = evaluate_outbound(
        _rules(),
        messages,
        declared_classification=DataClassification.INTERNAL,
        target_is_private_endpoint=True,
    )
    assert decision.decision == "DENY"
    assert "secret detected" in decision.reason
    assert decision.effective_classification is DataClassification.CONFIDENTIAL
    # Disabling secret detection keeps DLP raising: the payload still lands
    # at CONFIDENTIAL and flows into the classification gate.
    relaxed = evaluate_outbound(
        _rules(secret_detection_enabled=False, require_approval_above="INTERNAL"),
        messages,
        declared_classification=DataClassification.INTERNAL,
        target_is_private_endpoint=True,
    )
    assert relaxed.decision == "NEEDS_APPROVAL"
    assert relaxed.effective_classification is DataClassification.CONFIDENTIAL


def test_evaluate_outbound_denies_restricted_for_external_models() -> None:
    decision = evaluate_outbound(
        _rules(allow_restricted_to_private_endpoints=True),
        [{"role": "user", "content": "highest secret"}],
        declared_classification=DataClassification.RESTRICTED,
        target_is_private_endpoint=False,
    )
    assert decision.decision == "DENY"
    assert decision.reason == "restricted data cannot enter external models"
    allowed = evaluate_outbound(
        _rules(allow_restricted_to_private_endpoints=True),
        [{"role": "user", "content": "highest secret"}],
        declared_classification=DataClassification.RESTRICTED,
        target_is_private_endpoint=True,
    )
    assert allowed.decision == "ALLOW"


def test_evaluate_outbound_requires_approval_above_ceiling() -> None:
    decision = evaluate_outbound(
        _rules(require_approval_above="INTERNAL"),
        [{"role": "user", "content": "身份证 11010519491231002X"}],
        declared_classification=DataClassification.INTERNAL,
        target_is_private_endpoint=False,
    )
    assert decision.decision == "NEEDS_APPROVAL"


def test_evaluate_outbound_masks_configured_patterns() -> None:
    rules = _rules(mask_patterns=(MaskPattern(name="card", regex=r"\d{4}", replacement="****"),))
    decision = evaluate_outbound(
        rules,
        [{"role": "user", "content": "1234abcd"}],
        declared_classification=DataClassification.PUBLIC,
        target_is_private_endpoint=False,
    )
    assert decision.decision == "MASK"
    assert decision.masked_messages is not None
    assert decision.masked_messages[0]["content"] == "****abcd"
    untouched = evaluate_outbound(
        rules,
        [{"role": "user", "content": "no digits"}],
        declared_classification=DataClassification.PUBLIC,
        target_is_private_endpoint=False,
    )
    assert untouched.decision == "ALLOW"


def test_rules_reject_unknown_fields_and_bad_levels() -> None:
    with pytest.raises(ValidationError):
        GovernanceRules(max_outbound_classification="TOP_SECRET")  # type: ignore[arg-type]
    with pytest.raises(ValidationError):
        GovernanceRules(unexpected=True)  # type: ignore[call-arg]


def test_bundle_signature_binds_rules_and_version() -> None:
    rules = _rules()
    signature = sign_bundle(rules, 3, "signing-key")
    assert len(signature) == 64
    assert verify_bundle(rules, 3, signature, "signing-key")
    assert not verify_bundle(rules, 4, signature, "signing-key")
    assert not verify_bundle(
        _rules(allow_restricted_to_private_endpoints=True), 3, signature, "signing-key"
    )
    assert not verify_bundle(rules, 3, signature, "other-key")
    with pytest.raises(ValueError, match="signing key"):
        sign_bundle(rules, 3, "")
    assert compile_bundle(rules, 3)["governance"]["max_outbound_classification"] == "CONFIDENTIAL"


def test_canary_bucket_is_deterministic_in_range() -> None:
    key = str(uuid4())
    assert canary_bucket(key) == canary_bucket(key)
    assert 0 <= canary_bucket(key) < 100
    spread = {canary_bucket(f"{key}-{index}") for index in range(50)}
    assert len(spread) > 1
