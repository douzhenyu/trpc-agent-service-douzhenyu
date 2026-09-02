package platform.health

default allow := false

allow if {
    input.path == ["api", "v1", "health"]
    input.method == "GET"
}
