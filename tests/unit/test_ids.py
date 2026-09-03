from trpc_service.ids import uuid7


def test_uuid7_has_rfc_version_and_variant() -> None:
    identifier = uuid7()
    assert identifier.version == 7
    assert identifier.variant == "specified in RFC 4122"
