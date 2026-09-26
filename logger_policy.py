"""
logger_policy.py
Write-reduction policy for mqtt_logger.py. No I/O and no imports beyond the
stdlib, so it can be unit-tested (test_logger_policy.py) without a broker or DB.

Background: the logger used to run ~5 statements and 3 commits for every MQTT
message and insert a reading for every message, changed or not (~46 rows/s,
~85% of which repeated a value already in the table). StreamGate decides, per
key (a topic, a device), whether a given message is worth writing.

Rules, in priority order, for a message with value V on key K:
  1. First message ever seen for K                        -> write.
  2. >= `heartbeat` seconds since the last write for K    -> write (so constant
     values still show up in every chart bucket; keep heartbeat <= the smallest
     trend bucket, currently 5s).
  3. V equals the last value written                       -> skip.
  4. V changed, and K is a fast stream (its last two message gaps were both
     < `min_interval`)                                     -> write only if
     `min_interval` has passed since the last write (a rate cap; the next
     message of the stream carries the newer value anyway).
  5. V changed, K is NOT a fast stream                    -> write, always.

Rule 5 is what keeps sparse state topics correct (a bilge pump going 1 then 0
a second later): a change is never dropped unless a follow-up message is
already arriving every few hundred ms.
"""
import time


class StreamGate:
    def __init__(self, min_interval, heartbeat, clock=time.monotonic):
        self.min_interval = min_interval
        self.heartbeat = heartbeat
        self._clock = clock
        # key -> [last_msg_time, previous_gap, last_write_time, last_written_value]
        self._state = {}

    def allow(self, key, value):
        now = self._clock()
        st = self._state.get(key)
        if st is None:
            self._state[key] = [now, float('inf'), now, value]
            return True

        gap = now - st[0]
        prev_gap = st[1]
        st[0] = now
        st[1] = gap
        since_write = now - st[2]

        if since_write >= self.heartbeat:
            write = True
        elif value == st[3]:
            write = False
        else:
            streaming = gap < self.min_interval and prev_gap < self.min_interval
            write = not (streaming and since_write < self.min_interval)

        if write:
            st[2] = now
            st[3] = value
        return write
