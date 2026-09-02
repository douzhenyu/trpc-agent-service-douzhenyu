from http import HTTPStatus

import pytest

from dev.fake_external.scenarios import Scenario, plan_scenario


@pytest.mark.parametrize(
    ("scenario", "status", "delivery_count", "prepend", "delay", "disconnect"),
    [
        (Scenario.SUCCESS, HTTPStatus.OK, 1, False, 0, False),
        (Scenario.DUPLICATE, HTTPStatus.OK, 2, False, 0, False),
        (Scenario.OUT_OF_ORDER, HTTPStatus.OK, 1, True, 0, False),
        (Scenario.RATE_LIMIT, HTTPStatus.TOO_MANY_REQUESTS, 1, False, 0, False),
        (Scenario.TIMEOUT, HTTPStatus.GATEWAY_TIMEOUT, 1, False, 2, False),
        (Scenario.DISCONNECT, HTTPStatus.SERVICE_UNAVAILABLE, 1, False, 0, True),
        (Scenario.OUTCOME_UNKNOWN, HTTPStatus.ACCEPTED, 1, False, 0, False),
    ],
)
def test_external_failure_scenario_has_deterministic_transport_plan(
    scenario: Scenario,
    status: HTTPStatus,
    delivery_count: int,
    prepend: bool,
    delay: float,
    disconnect: bool,
) -> None:
    plan = plan_scenario(scenario)

    assert plan.status is status
    assert plan.delivery_count == delivery_count
    assert plan.prepend_delivery is prepend
    assert plan.delay_seconds == delay
    assert plan.disconnect is disconnect
