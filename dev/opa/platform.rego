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

private_endpoint if {
    startswith(input.endpoint_url, "https://internal.")
}

private_endpoint if {
    contains(input.endpoint_url, ".internal:")
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
    data.platform.governance.allow_restricted_to_private_endpoints == true
    private_endpoint
}
