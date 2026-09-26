"""Run with:  python3 -m unittest test_logger_policy -v"""
import unittest
from logger_policy import StreamGate


class FakeClock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t

    def advance(self, dt):
        self.t += dt


def feed(gate, clock, key, samples):
    """samples: [(seconds_since_previous_message, value)] -> list of write decisions."""
    out = []
    for dt, value in samples:
        clock.advance(dt)
        out.append(gate.allow(key, value))
    return out


class StreamGateTests(unittest.TestCase):
    def setUp(self):
        self.clock = FakeClock()
        self.gate = StreamGate(min_interval=2.0, heartbeat=5.0, clock=self.clock)

    def test_first_message_is_written(self):
        self.assertTrue(self.gate.allow('t', '1'))

    def test_unchanged_value_skipped_until_heartbeat(self):
        # constant value arriving every second: written at t=0, then every 5s
        result = feed(self.gate, self.clock, 't', [(0, 'x')] + [(1, 'x')] * 12)
        written_at = [i for i, w in enumerate(result) if w]
        self.assertEqual(written_at, [0, 5, 10])

    def test_fast_stream_is_rate_capped(self):
        # a value that changes on every message, 3 messages/second for 20s
        samples = [(0, '0')] + [(1 / 3, str(i)) for i in range(1, 61)]
        result = feed(self.gate, self.clock, 't', samples)
        writes = sum(result)
        # uncapped would be 61; capped it should be roughly one per min_interval
        self.assertLessEqual(writes, 14)
        self.assertGreaterEqual(writes, 8)

    def test_sparse_changes_are_never_dropped(self):
        # bilge pump: long silence, then 1, then 0 one second later.
        result = feed(self.gate, self.clock, 'bilge', [(0, '0'), (3600, '1'), (1, '0')])
        self.assertEqual(result, [True, True, True])

    def test_burst_of_two_quick_changes_after_silence_both_written(self):
        result = feed(self.gate, self.clock, 't', [(0, 'a'), (600, 'b'), (0.2, 'c')])
        self.assertEqual(result, [True, True, True])

    def test_keys_are_independent(self):
        self.assertTrue(self.gate.allow('a', '1'))
        self.assertTrue(self.gate.allow('b', '1'))
        self.clock.advance(0.5)
        self.assertFalse(self.gate.allow('a', '1'))
        self.assertFalse(self.gate.allow('b', '1'))

    def test_stream_latest_value_eventually_written(self):
        # a stream that stops on a value that was rate-capped away is picked up
        # by the next message after min_interval (changed vs. last *written*).
        samples = [(0, '0')] + [(0.3, str(i)) for i in range(1, 8)]
        feed(self.gate, self.clock, 't', samples)
        # mid-stream, right after a write: a changed value inside min_interval is capped...
        self.clock.advance(0.3)
        self.gate.allow('t', 'still-streaming')
        self.clock.advance(0.3)
        self.assertFalse(self.gate.allow('t', 'capped'))
        # ...but once min_interval has elapsed the next message goes through.
        self.clock.advance(2.5)
        self.assertTrue(self.gate.allow('t', 'later'))

    def test_touch_gate_limits_to_one_per_second(self):
        # how mqtt_logger uses it for mqtt_devices.last_seen (value always None)
        gate = StreamGate(1.0, 1.0, clock=self.clock)
        result = feed(gate, self.clock, 'dev', [(0, None)] + [(0.1, None)] * 25)
        self.assertEqual(sum(result), 3)  # t=0, ~1.0s, ~2.0s (within 2.6s total)


if __name__ == '__main__':
    unittest.main()
