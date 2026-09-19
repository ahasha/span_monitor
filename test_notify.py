from notify import Throttle


class FakeClock:
    """Manually advanced clock so tests never sleep."""

    def __init__(self, now=1000.0):
        self.now = now

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


def test_throttle_allows_first_call():
    assert Throttle(60.0, clock=FakeClock()).ready() is True


def test_throttle_blocks_second_immediate_call():
    throttle = Throttle(60.0, clock=FakeClock())
    assert throttle.ready() is True
    assert throttle.ready() is False


def test_throttle_allows_again_after_interval():
    clock = FakeClock()
    throttle = Throttle(60.0, clock=clock)
    assert throttle.ready() is True

    clock.advance(59.0)
    assert throttle.ready() is False

    clock.advance(1.0)
    assert throttle.ready() is True


def test_throttle_zero_interval_always_ready():
    """interval=0 disables throttling, for tests and debugging."""
    throttle = Throttle(0.0, clock=FakeClock())
    assert throttle.ready() is True
    assert throttle.ready() is True
