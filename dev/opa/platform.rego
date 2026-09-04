package platform.health

default allow := false

allow if {
    input.path == ["api", "v1", "health"]
    input.method == "GET"
}

package platform.llm

# Outbound model gate: governance decisions are deny-by-default and every
# allow/deny is explainable from the input. RESTRICTED data only leaves the
# platform to explicitly allow-listed private endpoints; the governing flags
# arrive through the signed Policy Bundle data document
# (data.platform.governance, absent data denies).

default allow := false

# Only scheme-pinned internal hosts count as private; substring matches on
# tenant-supplied URLs are spoofable and are intentionally not accepted.
private_endpoint if {
    startswith(input.endpoint_url, "https://internal.")
}

allow if {
    input.effective_classification in {"PUBLIC", "INTERNAL"}
}

allow if {
    input.effective_classification == "CONFIDENTIAL"
    private_endpoint
}

allow if {
    input.effective_classification == "RESTRICTED"
    input.allow_restricted_to_private_endpoints == true
    private_endpoint
}
