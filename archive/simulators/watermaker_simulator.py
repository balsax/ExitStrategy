#!/usr/bin/env python3
"""
Watermaker (RO system) simulator.

Simulates the full boat/watermaker/* controller: the automatic state machine
(idle -> priming -> ramping_up -> producing -> flushing -> idle), manual
device control, fault injection/bypass, and production-cycle tracking --
publishing the same MQTT topics a real controller would, and subscribing to
the same boat/watermaker/cmd/* command topics the dashboard already sends.

This mirrors the firmware behavior documented in dashboard_api.py and the
frontend's watermaker code (state names, valid transitions, fault vocabulary,
bypass semantics) closely enough that the dashboard can be fully developed
and tested -- gauges, trend charts, fault banners, the bypass panel, the
maintenance/manual control modal, production cycle counters -- without any
real hardware connected.

Usage:
  python3 watermaker_simulator.py
  python3 watermaker_simulator.py --fault-chance 0.01   # faults much more often
  python3 watermaker_simulator.py --tank-start 20        # start with a low tank

Send an MQTT command the same way the dashboard does, e.g.:
  mosquitto_pub -u USER -P PASS -t boat/watermaker/cmd/mode -m start

Debug-only (not part of the real device protocol -- lets you force a fault
on demand instead of waiting for --fault-chance's randomness):
  mosquitto_pub -u USER -P PASS -t boat/watermaker/sim/force_fault -m postfilter
  mosquitto_pub -u USER -P PASS -t boat/watermaker/sim/force_fault -m clear

Ctrl+C to stop.
"""
import argparse
import random
import threading
import time
import paho.mqtt.client as mqtt

MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_CLIENT_ID = "watermaker_simulator"

TOPIC_PREFIX = "boat/watermaker/"
CMD_PREFIX = TOPIC_PREFIX + "cmd/"
FORCE_FAULT_TOPIC = TOPIC_PREFIX + "sim/force_fault"

MODES = {'start', 'stop', 'flush', 'auto', 'manual', 'reset'}
DEVICES = ('pump', 'boost_pump', 'divert', 'flush')

# Short name (used on cmd/fault_bypass, fault_bypass_active) -> display text
# (used on fault_reasons_all/fault_reasons_bypassed) -- must match
# WM_BYPASSABLE_FAULTS in the frontend exactly, since the bypass panel keys
# off these display strings to decide "bypassed vs bypassed-but-still-true".
BYPASSABLE_FAULTS = {
    'product_sensor':  'Product sensor (Digmesa) communication lost',
    'postfilter':      'Postfilter pressure out of range',
    'tank_level':      'Tank level sensor out of range',
    'hp_pressure_low': 'HP pressure too low during production',
    'feed_starvation': 'Feed starvation during production',
}
# Hardware-protection fault that can only be cleared by 'reset' or 'manual' --
# never bypassable, per the real firmware (see dashboard_api.py comment on
# WATERMAKER_BYPASSABLE_FAULTS).
HP_OVERCURRENT = 'hp_overcurrent'
ALL_FAULTS = {**BYPASSABLE_FAULTS, HP_OVERCURRENT: 'HP pump current out of range'}

# Which of the valve/relay booleans matter in each automatic mode, and the
# steady-state target telemetry that mode smoothly approaches. 'ramping_up'
# sitting between 'priming' and 'producing' targets is what gives the ramp
# its shape -- the exponential smoothing in step() does the actual ramping,
# these are just the three waypoints it moves between.
TARGETS = {
    'idle':       dict(hp=0,   pre=0,  post=0,  flow=0,   cond=0,   current=0.0,  speed=0,  pump=0, boost=1),
    'priming':    dict(hp=90,  pre=18, post=16, flow=60,  cond=900, current=3.5,  speed=22, pump=1, boost=1),
    'ramping_up': dict(hp=550, pre=34, post=30, flow=350, cond=550, current=10.0, speed=55, pump=1, boost=1),
    'producing':  dict(hp=850, pre=42, post=38, flow=580, cond=380, current=14.5, speed=78, pump=1, boost=1),
    'flushing':   dict(hp=45,  pre=20, post=18, flow=260, cond=220, current=3.0,  speed=18, pump=1, boost=1),
    'fault':      dict(hp=0,   pre=0,  post=0,  flow=0,   cond=0,   current=0.0,  speed=0,  pump=0, boost=0),
}
# Divert only ever sends product to the tank once quality (conductivity) is
# good AND the system is actually producing -- otherwise it dumps overboard,
# same as a real product-water-quality diversion valve.
DIVERT_COND_THRESHOLD = 500.0
SMOOTHING_RATE = 0.15  # per second; ~6-7s time constant towards the current target
FEED_PUMP_ONLY_CURRENT_A = 1.2  # feed pump alone still draws current even with the HP pump off


def get_secrets():
    secrets = {}
    with open('/etc/dashboard/secrets.env') as f:
        for line in f:
            line = line.strip()
            if '=' in line and not line.startswith('#'):
                k, v = line.split('=', 1)
                secrets[k.strip()] = v.strip()
    return secrets


class WatermakerSimulator:
    def __init__(self, args):
        self.args = args
        self.lock = threading.Lock()
        self.rng = random.Random(args.seed)

        self.mode = 'idle'
        self.phase_start_t = time.time()
        self.production_active = False   # True while the current run (through its post-run flush) still counts
        self.cycle_start_t = None         # wall-clock start of the current production cycle, for current_time_sec

        self.total_time_sec = 0.0
        self.cycle_count = 0

        self.tank_pct = args.tank_start
        self.fault_bypass_config = set()  # operator-configured bypass, short names
        self.active_conditions = set()    # currently-true fault conditions, short names
        self.fault_rotate_idx = 0
        self.last_fault_rotate_t = time.time()
        self._forcing_sorted = []

        self.manual_devices = {d: 0 for d in DEVICES}
        self.manual_pump_speed = 0

        # Smoothed ("clean") telemetry -- noise is added only at publish time
        # so it doesn't feed back into the smoothing and random-walk away.
        self.hp = self.pre = self.post = self.flow = self.cond = 0.0
        self.current = 0.0
        self.speed = 0.0
        self.efficiency = 0.0
        self.pump_on = 0
        self.boost_on = 0
        self.divert_on = 0
        self.flush_on = 0

        self.last_tick_t = time.time()

    # ─── Command handling (called from the MQTT on_message callback) ──────────
    def handle_command(self, suffix, payload):
        with self.lock:
            if suffix == 'mode':
                self._handle_mode_cmd(payload)
            elif suffix in DEVICES:
                if payload in ('0', '1'):
                    self.manual_devices[suffix] = int(payload)
            elif suffix == 'pump_speed':
                try:
                    self.manual_pump_speed = max(0, min(100, int(payload)))
                except ValueError:
                    pass
            elif suffix == 'fault_bypass':
                self._handle_fault_bypass_cmd(payload)
            elif suffix == 'production_reset':
                if self.mode == 'idle':
                    self.total_time_sec = 0.0
                    self.cycle_count = 0

    def _handle_mode_cmd(self, cmd):
        if cmd not in MODES:
            return
        if cmd == 'start':
            if self.mode != 'idle':
                return
            self.mode = 'priming'
            self.phase_start_t = time.time()
            self.cycle_start_t = time.time()
            self.production_active = True
        elif cmd == 'flush':
            if self.mode != 'idle':
                return
            self.mode = 'flushing'
            self.phase_start_t = time.time()
            self.production_active = False  # standalone maintenance flush, not a production cycle
        elif cmd == 'stop':
            if self.mode in ('priming', 'ramping_up', 'producing'):
                self.mode = 'flushing'  # post-run rinse before idle, same as the real state machine
                self.phase_start_t = time.time()
            elif self.mode == 'flushing':
                self._finish_flush()  # abort remaining flush time immediately
        elif cmd == 'reset':
            if self.mode != 'fault':
                return
            self.active_conditions.clear()
            self.mode = 'idle'
            self.production_active = False
            self.cycle_start_t = None
        elif cmd == 'auto':
            if self.mode != 'manual':
                return
            self.mode = 'idle'
            self.manual_devices = {d: 0 for d in DEVICES}
        elif cmd == 'manual':
            # Accepted from any state, including fault -- manual control
            # overrides the automatic fault lockout entirely.
            self.mode = 'manual'
            self.active_conditions.clear()
            self.production_active = False
            self.cycle_start_t = None

    def _handle_fault_bypass_cmd(self, payload):
        if ':' not in payload:
            return
        name, state = payload.split(':', 1)
        if name not in BYPASSABLE_FAULTS or state not in ('0', '1'):
            return
        if state == '1':
            self.fault_bypass_config.add(name)
        else:
            self.fault_bypass_config.discard(name)

    def handle_force_fault(self, payload):
        """Debug-only hook (boat/watermaker/sim/force_fault) -- not part of the
        real device protocol, just a way to test faults/bypass deterministically."""
        with self.lock:
            name = payload.strip()
            if name == 'clear':
                self.active_conditions.clear()
            elif name in ALL_FAULTS:
                self.active_conditions.add(name)

    def _finish_flush(self):
        if self.production_active and self.cycle_start_t is not None:
            self.total_time_sec += time.time() - self.cycle_start_t
            self.cycle_count += 1
        self.mode = 'idle'
        self.production_active = False
        self.cycle_start_t = None
        self.manual_devices = {d: 0 for d in DEVICES}

    # ─── Per-tick simulation ───────────────────────────────────────────────────
    def step(self):
        with self.lock:
            now = time.time()
            dt = now - self.last_tick_t
            self.last_tick_t = now
            elapsed_in_phase = now - self.phase_start_t

            # Automatic phase advancement.
            if self.mode == 'priming' and elapsed_in_phase >= self.args.priming_duration:
                self.mode = 'ramping_up'
                self.phase_start_t = now
            elif self.mode == 'ramping_up' and elapsed_in_phase >= self.args.ramp_duration:
                self.mode = 'producing'
                self.phase_start_t = now
            elif self.mode == 'flushing':
                duration = self.args.flush_duration if self.production_active else self.args.standalone_flush_duration
                if elapsed_in_phase >= duration:
                    self._finish_flush()

            # Random fault injection while actually producing -- a fault
            # mid-priming/ramp would just be confusing to test against.
            if self.mode == 'producing' and self.args.fault_chance > 0:
                if self.rng.random() < self.args.fault_chance * dt:
                    candidates = [f for f in ALL_FAULTS if f not in self.active_conditions]
                    if candidates:
                        self.active_conditions.add(self.rng.choice(candidates))

            forcing = {c for c in self.active_conditions if c not in self.fault_bypass_config}
            if forcing and self.mode not in ('fault', 'manual'):
                self.mode = 'fault'
                self.phase_start_t = now

            self._update_telemetry(dt)
            self._update_tank(dt)
            self._rotate_fault_reason(now, forcing)

            return self._snapshot(forcing)

    def _update_telemetry(self, dt):
        if self.mode == 'manual':
            pump_on = self.manual_devices['pump']
            boost_on = self.manual_devices['boost_pump']
            frac = (self.manual_pump_speed / 100.0) if pump_on else 0.0
            full = TARGETS['producing']
            # Feed-side pressure (pre/postfilter) comes from the feed pump, not
            # the HP pump -- it should rise as soon as the feed pump is on,
            # independent of whether the HP pump is running or how fast.
            feed_frac = 1.0 if boost_on else 0.0
            target = dict(
                hp=full['hp'] * frac, pre=full['pre'] * feed_frac, post=full['post'] * feed_frac,
                flow=full['flow'] * frac, cond=full['cond'] if pump_on else 0,
                current=full['current'] * frac + (FEED_PUMP_ONLY_CURRENT_A if boost_on and not pump_on else 0),
                speed=self.manual_pump_speed if pump_on else 0,
            )
            pump_on_flag, boost_on_flag = pump_on, boost_on
            divert_flag, flush_flag = self.manual_devices['divert'], self.manual_devices['flush']
        else:
            target = TARGETS[self.mode]
            pump_on_flag, boost_on_flag = target['pump'], target['boost']
            flush_flag = 1 if self.mode == 'flushing' else 0
            divert_flag = 1 if (self.mode == 'producing' and self.cond < DIVERT_COND_THRESHOLD) else 0

        k = min(1.0, SMOOTHING_RATE * dt)
        self.hp += (target['hp'] - self.hp) * k
        self.pre += (target['pre'] - self.pre) * k
        self.post += (target['post'] - self.post) * k
        self.flow += (target['flow'] - self.flow) * k
        self.cond += (target['cond'] - self.cond) * k
        self.current += (target['current'] - self.current) * k
        self.speed += (target['speed'] - self.speed) * k

        eff_target = 27.0 if self.mode in ('producing', 'manual') and pump_on_flag else 0.0
        self.efficiency += (eff_target - self.efficiency) * k

        self.pump_on, self.boost_on = pump_on_flag, boost_on_flag
        self.divert_on, self.flush_on = divert_flag, flush_flag

    def _update_tank(self, dt):
        drain_pct_s = self.args.tank_drain_pct_per_hour / 3600.0
        self.tank_pct -= drain_pct_s * dt
        if self.divert_on and self.flow > 0:
            fill_gph = self.flow * 60 / 3785.411784  # mL/min -> gal/hr
            self.tank_pct += (fill_gph / self.args.tank_capacity_gal) * 100.0 / 3600.0 * dt
        self.tank_pct = max(0.0, min(100.0, self.tank_pct))

    def _rotate_fault_reason(self, now, forcing):
        if now - self.last_fault_rotate_t >= 2.0:
            self.last_fault_rotate_t = now
            self.fault_rotate_idx += 1
        self._forcing_sorted = sorted(forcing)

    def current_fault_reason(self):
        if not self._forcing_sorted:
            return ''
        return ALL_FAULTS[self._forcing_sorted[self.fault_rotate_idx % len(self._forcing_sorted)]]

    def _snapshot(self, forcing):
        bypassed_true = {c for c in self.active_conditions if c in self.fault_bypass_config}
        return {
            'mode': self.mode,
            'hp': self.hp, 'pre': self.pre, 'post': self.post,
            'flow': self.flow, 'cond': self.cond, 'current': self.current, 'speed': self.speed,
            'efficiency': self.efficiency, 'tank_pct': self.tank_pct,
            'pump_on': self.pump_on, 'boost_on': self.boost_on,
            'divert_on': self.divert_on, 'flush_on': self.flush_on,
            'fault_reasons_all': ','.join(ALL_FAULTS[c] for c in sorted(forcing)),
            'fault_reason': self.current_fault_reason(),
            'fault_reasons_bypassed': ','.join(BYPASSABLE_FAULTS[c] for c in sorted(bypassed_true) if c in BYPASSABLE_FAULTS),
            'fault_bypass_active': ','.join(sorted(self.fault_bypass_config)),
            'current_time_sec': (time.time() - self.cycle_start_t) if self.cycle_start_t else 0.0,
            'total_time_sec': self.total_time_sec,
            'cycle_count': self.cycle_count,
        }


def publish(client, s):
    noise = lambda v, pct: v + random.uniform(-pct, pct) * max(abs(v), 1)
    client.publish(TOPIC_PREFIX + 'mode', s['mode'])
    client.publish(TOPIC_PREFIX + 'pressure/hp', f"{noise(s['hp'], 0.01):.1f}")
    client.publish(TOPIC_PREFIX + 'pressure/prefilter', f"{noise(s['pre'], 0.01):.1f}")
    client.publish(TOPIC_PREFIX + 'pressure/postfilter', f"{noise(s['post'], 0.01):.1f}")
    client.publish(TOPIC_PREFIX + 'flow/rate', f"{noise(s['flow'], 0.015):.1f}")
    client.publish(TOPIC_PREFIX + 'flow/conductivity_comp', f"{noise(s['cond'], 0.01):.1f}")
    client.publish(TOPIC_PREFIX + 'pump/current', f"{noise(s['current'], 0.02):.2f}")
    client.publish(TOPIC_PREFIX + 'pump/speed_pct', f"{s['speed']:.1f}")
    client.publish(TOPIC_PREFIX + 'pump/rpm', "0")  # no sensor installed, per the real firmware
    client.publish(TOPIC_PREFIX + 'efficiency', f"{s['efficiency']:.1f}")
    client.publish(TOPIC_PREFIX + 'tank/level', f"{s['tank_pct']:.1f}")
    client.publish(TOPIC_PREFIX + 'pump/state', str(s['pump_on']))
    client.publish(TOPIC_PREFIX + 'boost_pump/state', str(s['boost_on']))
    client.publish(TOPIC_PREFIX + 'divert/state', str(s['divert_on']))
    client.publish(TOPIC_PREFIX + 'flush/state', str(s['flush_on']))
    client.publish(TOPIC_PREFIX + 'fault_reasons_all', s['fault_reasons_all'])
    client.publish(TOPIC_PREFIX + 'fault_reason', s['fault_reason'])
    client.publish(TOPIC_PREFIX + 'fault_reasons_bypassed', s['fault_reasons_bypassed'])
    client.publish(TOPIC_PREFIX + 'fault_bypass_active', s['fault_bypass_active'])
    client.publish(TOPIC_PREFIX + 'production/current_time_sec', f"{s['current_time_sec']:.0f}")
    client.publish(TOPIC_PREFIX + 'production/total_time_sec', f"{s['total_time_sec']:.0f}")
    client.publish(TOPIC_PREFIX + 'production/cycle_count', str(s['cycle_count']))
    client.publish(TOPIC_PREFIX + 'status', 'online')


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--rate', type=float, default=1.0, help='Publish interval in seconds (default 1.0)')
    ap.add_argument('--tank-start', type=float, default=55.0, help='Starting tank level %% (default 55)')
    ap.add_argument('--tank-capacity-gal', type=float, default=100.0, help='Fresh water tank capacity in gallons (default 100)')
    ap.add_argument('--tank-drain-pct-per-hour', type=float, default=2.0, help='Background consumption drain rate, %%/hour (default 2.0)')
    ap.add_argument('--priming-duration', type=float, default=10.0, help='Seconds spent priming before ramping up (default 10)')
    ap.add_argument('--ramp-duration', type=float, default=20.0, help='Seconds spent ramping up before producing (default 20)')
    ap.add_argument('--flush-duration', type=float, default=25.0, help='Post-production flush duration in seconds (default 25)')
    ap.add_argument('--standalone-flush-duration', type=float, default=18.0, help='Standalone maintenance flush duration, no prior production (default 18)')
    ap.add_argument('--fault-chance', type=float, default=0.0006, help='Probability per second of a new fault while producing (default 0.0006, ~ one every ~28min)')
    ap.add_argument('--seed', type=int, default=None, help='Random seed for reproducible fault timing')
    args = ap.parse_args()

    secrets = get_secrets()
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=MQTT_CLIENT_ID)
    client.username_pw_set(secrets['MQTT_USER'], secrets['MQTT_PASS'])

    sim = WatermakerSimulator(args)

    def on_connect(c, userdata, flags, reason_code, properties):
        c.subscribe(CMD_PREFIX + '#')
        c.subscribe(FORCE_FAULT_TOPIC)

    def on_message(c, userdata, msg):
        payload = msg.payload.decode('utf-8', errors='ignore').strip()
        if msg.topic == FORCE_FAULT_TOPIC:
            sim.handle_force_fault(payload)
        elif msg.topic.startswith(CMD_PREFIX):
            sim.handle_command(msg.topic[len(CMD_PREFIX):], payload)

    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(MQTT_BROKER, MQTT_PORT, keepalive=30)
    client.loop_start()

    print(f"Simulating watermaker RO system -- tank starts at {args.tank_start:.0f}%, "
          f"fault chance {args.fault_chance}/s while producing")
    print(f"Publishing to {TOPIC_PREFIX}* and listening on {CMD_PREFIX}# — Ctrl+C to stop\n")

    try:
        while True:
            s = sim.step()
            publish(client, s)
            faults = s['fault_reasons_all']
            fault_note = f" FAULT: {faults}" if faults else ''
            print(f"\r[{s['mode']:<10}] hp={s['hp']:6.1f}psi flow={s['flow']*60/3785.411784:5.2f}gph "
                  f"cond={s['cond']:5.1f} tank={s['tank_pct']:5.1f}% cyc={s['cycle_count']}{fault_note}          ",
                  end='', flush=True)
            time.sleep(args.rate)
    except KeyboardInterrupt:
        print("\nStopped.")
    finally:
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    main()
