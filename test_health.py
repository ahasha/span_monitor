import health
from health import HealthState


class FakeClock:
    """Manually advanced clock so tests never sleep."""

    def __init__(self, now=1000.0):
        self.now = now

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


def test_cold_start_is_unhealthy():
    """Before the first successful write there is nothing to trust."""
    state = HealthState(stale_threshold=300.0, clock=FakeClock())
    assert state.is_healthy() is False
    assert state.seconds_since_success() is None


def test_fresh_success_is_healthy():
    clock = FakeClock()
    state = HealthState(stale_threshold=300.0, clock=clock)
    state.record_success()
    clock.advance(10.0)

    assert state.is_healthy() is True
    assert state.seconds_since_success() == 10.0


def test_success_older_than_threshold_is_unhealthy():
    clock = FakeClock()
    state = HealthState(stale_threshold=300.0, clock=clock)
    state.record_success()
    clock.advance(301.0)

    assert state.is_healthy() is False


def test_exactly_at_threshold_is_still_healthy():
    """The boundary is inclusive, so a tick landing exactly on it is fine."""
    clock = FakeClock()
    state = HealthState(stale_threshold=300.0, clock=clock)
    state.record_success()
    clock.advance(300.0)

    assert state.is_healthy() is True


def test_failures_accumulate_and_success_resets_them():
    state = HealthState(stale_threshold=300.0, clock=FakeClock())
    state.record_failure("first")
    state.record_failure("second")
    assert state.snapshot()["consecutive_errors"] == 2
    assert state.snapshot()["last_error"] == "second"

    state.record_success()
    assert state.snapshot()["consecutive_errors"] == 0


def test_failures_do_not_make_a_fresh_state_unhealthy():
    """Errors alone don't mean unhealthy — the retry layer is expected to
    hit transient errors. Only sustained absence of success does."""
    clock = FakeClock()
    state = HealthState(stale_threshold=300.0, clock=clock)
    state.record_success()
    clock.advance(5.0)
    state.record_failure("transient")

    assert state.is_healthy() is True


def test_snapshot_shape():
    state = HealthState(stale_threshold=300.0, clock=FakeClock())
    state.record_success()

    assert state.snapshot() == {
        "healthy": True,
        "seconds_since_success": 0.0,
        "consecutive_errors": 0,
        "last_error": "",
    }
