"""Pure scenario state machine shared by the Fake LLM and Fake IM endpoints."""

from dataclasses import dataclass
from enum import StrEnum
from http import HTTPStatus


class Scenario(StrEnum):
    SUCCESS = "success"
    DUPLICATE = "duplicate"
    OUT_OF_ORDER = "out_of_order"
    RATE_LIMIT = "rate_limit"
    TIMEOUT = "timeout"
    DISCONNECT = "disconnect"
    OUTCOME_UNKNOWN = "outcome_unknown"


@dataclass(frozen=True)
class ScenarioPlan:
    status: HTTPStatus
    delivery_count: int = 1
    prepend_delivery: bool = False
    delay_seconds: float = 0
    disconnect: bool = False


def plan_scenario(scenario: Scenario) -> ScenarioPlan:
    """Translate a requested failure scenario into deterministic transport behavior."""
    match scenario:
        case Scenario.SUCCESS:
            return ScenarioPlan(status=HTTPStatus.OK)
        case Scenario.DUPLICATE:
            return ScenarioPlan(status=HTTPStatus.OK, delivery_count=2)
        case Scenario.OUT_OF_ORDER:
            return ScenarioPlan(status=HTTPStatus.OK, prepend_delivery=True)
        case Scenario.RATE_LIMIT:
            return ScenarioPlan(status=HTTPStatus.TOO_MANY_REQUESTS)
        case Scenario.TIMEOUT:
            return ScenarioPlan(status=HTTPStatus.GATEWAY_TIMEOUT, delay_seconds=2)
        case Scenario.DISCONNECT:
            return ScenarioPlan(status=HTTPStatus.SERVICE_UNAVAILABLE, disconnect=True)
        case Scenario.OUTCOME_UNKNOWN:
            return ScenarioPlan(status=HTTPStatus.ACCEPTED)

    raise AssertionError(f"unhandled scenario: {scenario}")
