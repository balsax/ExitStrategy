#!/usr/bin/env python3
"""
Victron DC-side simulator for the Electrical tab.

Stands in for four devices that aren't installed yet, so the Electrical
tab's flow diagram and node popups can be built and tested now:

  shunt_diesel    SmartShunt 500A  on the 12V diesel start battery
  shunt_thruster  SmartShunt 1000A on the 12V bow thruster battery
  mppt            SmartSolar MPPT 150/35 charging the 48V house bank
  orion_alt       Orion 12|48 DC-DC charging the 48V house bank from the
                  diesel alternator (engine-detect: only runs while the
                  engine is charging the 12V side)

Publishes plain values to boat/sim/victron/<device>/<field>. The dev
dashboard's /api/victron/sim route (a siloed block in dashboard_api.py)
flattens those into <device>_<field> keys for the frontend, and ignores
anything older than 30s -- so stopping this script makes the nodes go
back to "--" on their own. The boat/sim/ prefix is in mqtt_logger.py's
IGNORE_PREFIXES, so nothing here ends up in MariaDB as a fake device.

This is NOT the format real hardware will arrive in. Once installed, these
devices report through the Cerbo/VRM like the existing 12V house SmartShunt,
and get_vrm_data() will need to tell them apart by device instance (VRM uses
the same attribute codes, e.g. 'bv', for every battery monitor).

What it models (everything is a best guess -- tune the constants below):
  - Engine: off most of the time; in --engine auto it runs for a few
    minutes every so often (demo pace, not real-life pace). A start pulls
    a few hundred amps off the diesel battery for ~2s.
  - Alternator: feeds the diesel battery (tapering as it refills), the
    Orion 12|48, and a small engine load. Its current is an estimate --
    nothing on the boat will actually measure it.
  - Orion 12|48: turns on a few seconds after the input rises above its
    engine-detect voltage, Bulk then Absorption, off when the engine stops.
  - Bow thruster: short bursts of several hundred amps while the engine
    is running (docking/maneuvering), recharged from the diesel side
    afterwards.
  - Orion-Tr Smart 48/12 x2 (orion_house, orion_thruster): only what their
    Bluetooth Instant Readout carries (state, voltages, off reason, error,
    signal) -- same fields victron_ble_bridge.py publishes, no current. The
    house one sits in Float with an occasional Bulk/Absorption cycle; the
    thruster one charges the thruster battery (and the diesel battery
    through the Cyrix) with the engine off, and is locked out by its
    remote input while the engine runs (so the Orions never loop power
    48V -> 12V -> 48V).
  - 48V DC loads (dc48_load): watermaker runs + a 48V A/C compressor
    cycling. Total only (nothing will meter them individually yet).
  - 12V house loads (house_load): a fridge compressor cycling on/off, a
    laptop charging, always-on nav gear, and small lights/pumps/electronics.
    Only the total is published (nothing meters individual appliances);
    it gives the Electrical tab's 12V Loads card something realistic to show.
  - Cyrix between the diesel and thruster batteries: with the engine
    running the thruster charges off the alternator through it. It
    reports nothing; the dashboard infers it from engine running.
  - Bimini solar (mppt_bimini): a second, 400W array on its own
    SmartSolar MPPT 100/20, same fields as mppt. Gets extra shading (boom/sail shadow
    sweeping across it as the boat swings).
  - Solar: follows the local time of day (sunrise/sunset below) with
    passing clouds; Bulk in the morning, Absorption around midday, Float
    in the afternoon. --daylight forces midday sun for testing at night.

Usage:
  python3 victron_simulator.py
  python3 victron_simulator.py --engine on --daylight
  python3 victron_simulator.py --engine off --rate 2

Ctrl+C to stop.
"""
import argparse
import math
import random
import time
from datetime import datetime

import paho.mqtt.client as mqtt

MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_CLIENT_ID = "victron_simulator"
TOPIC_PREFIX = "boat/sim/victron"

# ── Battery assumptions (not known yet -- adjust to the real batteries) ──────
# AGM-style 12V batteries: open-circuit voltage from SOC, internal
# resistance for sag under load / rise under charge.
BATTERIES = {
    'shunt_diesel':   {'rating_a': 500,  'capacity_ah': 100.0, 'soc': 92.0, 'r_ohm': 0.004},
    'shunt_thruster': {'rating_a': 1000, 'capacity_ah': 100.0, 'soc': 96.0, 'r_ohm': 0.0025},
}
OCV_EMPTY, OCV_FULL = 11.8, 12.85      # AGM resting voltage at 0% / 100% SOC
CHARGE_V_MAX = 14.4                     # alternator/charger absorption voltage
FLOAT_V = 13.6
LOW_V_ALARM = 11.0                      # SmartShunt low-voltage alarm ...
LOW_V_ALARM_DELAY_S = 10                # ... only after this long (ignores start sag)

# ── Engine / alternator ─────────────────────────────────────────────────────
ALT_MAX_A = 115.0                       # alternator rating
ENGINE_LOAD_A = 3.0                     # engine electrics (fuel pump, instruments)
START_CURRENT_A = (180.0, 350.0)
START_DURATION_S = 2
AUTO_ENGINE_OFF_S = (360, 720)          # --engine auto: off for 6-12 min ...
AUTO_ENGINE_ON_S = (240, 480)           # ... then runs for 4-8 min

# ── Orion 12|48 ─────────────────────────────────────────────────────────────
ORION_MAX_OUT_A = 8.0                   # output current limit at 48V -- set to the unit's rating
ORION_EFFICIENCY = 0.88
ORION_ENGINE_ON_V = 13.2                # engine-detect start threshold
ORION_START_DELAY_S = 5
ORION_BULK_S = 180                      # then Absorption, tapering toward ORION_ABS_MIN_A
ORION_ABS_MIN_A = 3.0

# ── Bow thruster ────────────────────────────────────────────────────────────
THRUSTER_CHANCE = 0.012                 # per second while the engine runs
THRUSTER_BURST_S = (3, 12)
THRUSTER_CURRENT_A = (250.0, 550.0)
THRUSTER_RECHARGE_MAX_A = 25.0          # through the Cyrix off the alternator while the engine runs

# ── Solar / MPPT 150/35 ─────────────────────────────────────────────────────
ARRAY_WP = 1200.0                       # arch array (mppt) -- adjust
MPPT_MAX_OUT_A = 35.0
BIMINI_WP = 400.0                       # bimini array (mppt_bimini)
BIMINI_MPPT_MAX_A = 20.0                # SmartSolar MPPT 100/20 (48V-capable) output limit
# 2 x 200W panels in series: PV must sit well above the 48V bank for the MPPT to charge
BIMINI_PV_VMP, BIMINI_PV_VOC = 80.0, 92.0
SUNRISE_H, SUNSET_H = 7.3, 19.3         # local hours (St. Pete, late September)
PV_VMP, PV_VOC = 105.0, 125.0
BANK48_V = {'Bulk': 53.6, 'Absorption': 56.4, 'Float': 54.0}

MPPT_STATE_CODES = {'Off': 0, 'Bulk': 3, 'Absorption': 4, 'Float': 5}

# ── Orion-Tr Smart 48/12-30A x2 (Bluetooth Instant Readout fields only) ─────
ORION48_FLOAT_V, ORION48_ABS_V = 13.5, 14.2
HOUSE_CYCLE_CHANCE = 0.002              # per second in Float: 12V house load pulls it into Bulk
HOUSE_BULK_S, HOUSE_ABS_S = (60, 180), (120, 300)
ORION48_MAX_A = 30.0                    # Orion-Tr Smart 48/12-30A
ORION_RSSI = {'orion_house': -68, 'orion_thruster': -79}

# ── 12V house loads (amps at 12V) ───────────────────────────────────────────
FRIDGE_A = 4.5                          # compressor running
FRIDGE_ON_S, FRIDGE_OFF_S = (180, 420), (300, 720)
LAPTOP_A = 3.5                          # 12V laptop charger
NAV_A = 2.2                             # plotter, instruments, VHF + AIS standby
OTHER_A = (0.2, 2.5)                    # lights, water pump bursts, phone chargers

# ── 48V DC loads (watts) ────────────────────────────────────────────────────
WATERMAKER_W = 650.0
WATERMAKER_ON_S, WATERMAKER_OFF_S = (240, 480), (600, 1200)   # demo pace
AC_W = 750.0                            # 48V air conditioner, compressor running
AC_ON_S, AC_OFF_S = (240, 420), (180, 360)
AC_FAN_W = 45.0                         # fan only between compressor cycles


def get_secrets():
    secrets = {}
    with open('/etc/dashboard/secrets.env') as f:
        for line in f:
            line = line.strip()
            if '=' in line and not line.startswith('#'):
                k, v = line.split('=', 1)
                secrets[k.strip()] = v.strip()
    return secrets


def ocv(soc):
    return OCV_EMPTY + (OCV_FULL - OCV_EMPTY) * soc / 100.0


def charge_acceptance(soc, max_a):
    """How much current a battery will take -- full rate when low, tapering to ~1A near full."""
    if soc >= 99.5:
        return 0.5
    return max(1.0, max_a * min(1.0, (100.0 - soc) / 15.0))


def battery_voltage(b, current):
    base = ocv(b['soc'])
    if current > 0.5:
        return min(CHARGE_V_MAX, base + 0.9 + current * b['r_ohm'])
    if current > 0:
        return max(base, FLOAT_V) if b['soc'] > 99 else base
    return base + current * b['r_ohm']


def sun_fraction(now, force_daylight):
    """0..1 clear-sky irradiance for the local time of day."""
    if force_daylight:
        return 1.0
    h = now.hour + now.minute / 60.0 + now.second / 3600.0
    if h <= SUNRISE_H or h >= SUNSET_H:
        return 0.0
    return math.sin(math.pi * (h - SUNRISE_H) / (SUNSET_H - SUNRISE_H)) ** 1.3


class Sim:
    def __init__(self, engine_mode, force_daylight):
        self.engine_mode = engine_mode
        self.force_daylight = force_daylight
        self.engine_on = engine_mode == 'on'
        self.engine_timer = random.uniform(*AUTO_ENGINE_OFF_S) if engine_mode == 'auto' else 0
        self.engine_runtime = 0.0
        self.start_left = 0.0
        self.start_current = 0.0
        self.orion_on_time = 0.0
        self.thruster_left = 0.0
        self.thruster_current = 0.0
        self.cloud = 1.0
        self.cloud_target = 1.0
        self.yield_today = 0.0          # kWh
        self.yield_total = 1234.5       # kWh, arbitrary lifetime counter
        self.max_power_today = 0.0
        self.b_yield_today, self.b_yield_total, self.b_max_power_today = 0.0, 412.3, 0.0
        self.b_shade, self.b_shade_target = 1.0, 1.0
        self.day = datetime.now().date()
        self.low_v_since = {k: None for k in BATTERIES}
        self.house_orion_state, self.house_orion_left = 'Float', 0.0
        self.fridge_on, self.fridge_left = True, random.uniform(*FRIDGE_ON_S)
        self.other_a = 0.8
        self.wm_on, self.wm_left = False, random.uniform(*WATERMAKER_OFF_S) / 4
        self.ac_on, self.ac_left = True, random.uniform(*AC_ON_S)

    def step(self, dt):
        now = datetime.now()
        if now.date() != self.day:
            self.day, self.yield_today, self.max_power_today = now.date(), 0.0, 0.0
            self.b_yield_today, self.b_max_power_today = 0.0, 0.0

        # ── engine on/off ──
        was_on = self.engine_on
        if self.engine_mode == 'auto':
            self.engine_timer -= dt
            if self.engine_timer <= 0:
                self.engine_on = not self.engine_on
                self.engine_timer = random.uniform(*(AUTO_ENGINE_ON_S if self.engine_on else AUTO_ENGINE_OFF_S))
        if self.engine_on and not was_on:
            self.start_left = START_DURATION_S
            self.start_current = random.uniform(*START_CURRENT_A)
            self.engine_runtime = 0.0
            print(f"{now:%H:%M:%S}  engine start ({self.start_current:.0f}A crank)")
        if was_on and not self.engine_on:
            print(f"{now:%H:%M:%S}  engine stop")
        running = self.engine_on and self.start_left <= 0
        if running:
            self.engine_runtime += dt

        # ── thruster bursts (only while maneuvering under power) ──
        if self.thruster_left > 0:
            self.thruster_left -= dt
        elif running and random.random() < THRUSTER_CHANCE * dt:
            self.thruster_left = random.uniform(*THRUSTER_BURST_S)
            self.thruster_current = random.uniform(*THRUSTER_CURRENT_A)
            print(f"{now:%H:%M:%S}  bow thruster {self.thruster_current:.0f}A for {self.thruster_left:.0f}s")
        thrusting = self.thruster_left > 0

        diesel, thruster = BATTERIES['shunt_diesel'], BATTERIES['shunt_thruster']

        # ── Orion 12|48 (engine-detect on the diesel side's voltage) ──
        diesel_v_guess = battery_voltage(diesel, 5.0) if running else ocv(diesel['soc'])
        if running and diesel_v_guess >= ORION_ENGINE_ON_V:
            self.orion_on_time += dt
        else:
            self.orion_on_time = 0.0
        orion_active = self.orion_on_time >= ORION_START_DELAY_S
        if not orion_active:
            orion_state, orion_out_a = 'Off', 0.0
            orion_off_reason = 'Engine shutdown detected' if not running else 'Starting up'
        else:
            t = self.orion_on_time - ORION_START_DELAY_S
            orion_off_reason = 'None'
            if t < ORION_BULK_S:
                orion_state, orion_out_a = 'Bulk', ORION_MAX_OUT_A
            else:
                orion_state = 'Absorption'
                orion_out_a = max(ORION_ABS_MIN_A, ORION_MAX_OUT_A * math.exp(-(t - ORION_BULK_S) / 600.0))
            orion_out_a *= random.uniform(0.97, 1.02)
        orion_out_v = BANK48_V['Absorption'] if orion_state == 'Absorption' else BANK48_V['Bulk'] + random.uniform(-0.1, 0.2)
        orion_out_w = orion_out_a * orion_out_v

        # ── diesel + thruster battery currents ──
        thruster_i = -self.thruster_current if thrusting else -0.02
        if self.start_left > 0:
            self.start_left -= dt
            diesel_i, alt_a = -self.start_current * random.uniform(0.85, 1.0), 0.0
        elif running:
            diesel_accept = charge_acceptance(diesel['soc'], 60.0)
            thr_accept = 0.0 if thrusting else charge_acceptance(thruster['soc'], THRUSTER_RECHARGE_MAX_A)
            orion_in_a = orion_out_w / ORION_EFFICIENCY / CHARGE_V_MAX
            demand = ENGINE_LOAD_A + orion_in_a + thr_accept + diesel_accept
            alt_a = min(ALT_MAX_A, demand)
            spare = alt_a - ENGINE_LOAD_A - orion_in_a
            thr_share = min(thr_accept, max(0.0, spare))
            diesel_i = spare - thr_share
            if not thrusting:
                thruster_i = thr_share
        else:
            # Engine off: the thruster Orion (48->12) charges the thruster
            # battery, and the diesel battery through the Cyrix (which its
            # charge voltage closes), sharing its 30A.
            alt_a = 0.0
            thr_want = 0.0 if thrusting else charge_acceptance(thruster['soc'], ORION48_MAX_A)
            dsl_want = charge_acceptance(diesel['soc'], ORION48_MAX_A)
            scale = min(1.0, ORION48_MAX_A / max(thr_want + dsl_want, 0.01))
            diesel_i = dsl_want * scale
            if not thrusting:
                thruster_i = thr_want * scale * random.uniform(0.97, 1.02)
        diesel_i *= random.uniform(0.98, 1.02)

        # ── solar ──
        sun = sun_fraction(now, self.force_daylight)
        if random.random() < 0.01 * dt:
            self.cloud_target = random.choice([1.0, 1.0, 0.9, 0.6, 0.35])
        self.cloud += (self.cloud_target - self.cloud) * min(1.0, 0.15 * dt)
        pv_available = ARRAY_WP * sun * self.cloud * random.uniform(0.97, 1.02)
        h = now.hour + now.minute / 60.0
        if self.force_daylight:
            h = 12.0
        if pv_available < 5:
            mppt_state = 'Off'
        elif h < 11:
            mppt_state = 'Bulk'
        elif h < 13.5:
            mppt_state = 'Absorption'
        else:
            mppt_state = 'Float'
        bat48_v = BANK48_V.get(mppt_state, 53.2) + random.uniform(-0.05, 0.05)
        limit = {'Bulk': 1.0, 'Absorption': 0.7, 'Float': 0.35}.get(mppt_state, 0.0)
        pv_w = min(pv_available, pv_available * limit, MPPT_MAX_OUT_A * bat48_v / 0.97)
        out_a = pv_w * 0.97 / bat48_v if pv_w > 0 else 0.0
        if sun <= 0:
            pv_v = 0.0
        elif pv_w < 5:
            pv_v = PV_VOC * min(1.0, sun * 20)
        else:
            pv_v = PV_VMP + (PV_VOC - PV_VMP) * (1 - limit) * 0.6 + random.uniform(-1, 1)
        self.yield_today += pv_w * 0.97 * dt / 3.6e6
        self.yield_total += pv_w * 0.97 * dt / 3.6e6
        self.max_power_today = max(self.max_power_today, pv_w)

        # ── integrate SOC + shunt outputs ──
        out = {}
        for name, b, i in (('shunt_diesel', diesel, diesel_i), ('shunt_thruster', thruster, thruster_i)):
            b['soc'] = max(0.0, min(100.0, b['soc'] + i * dt / 3600.0 / b['capacity_ah'] * 100.0))
            v = battery_voltage(b, i)
            if v < LOW_V_ALARM:
                self.low_v_since[name] = self.low_v_since[name] or time.time()
            else:
                self.low_v_since[name] = None
            alarm = ('Low voltage' if self.low_v_since[name]
                     and time.time() - self.low_v_since[name] >= LOW_V_ALARM_DELAY_S else 'None')
            remaining_ah = b['capacity_ah'] * b['soc'] / 100.0
            out[name] = {
                'voltage': round(v, 2),
                'current': round(i, 2),
                'power': round(v * i, 1),
                'soc': round(b['soc'], 1),
                'consumed_ah': round(-(b['capacity_ah'] - remaining_ah), 1),
                # Minutes, like the 12V house shunt's field; '--' when not discharging
                'ttg': round(remaining_ah / -i * 60) if i < -0.1 else '--',
                'alarm': alarm,
                'rating': b['rating_a'],
            }

        out['engine'] = {
            'running': 1 if self.engine_on else 0,
            'alternator_current': round(alt_a * random.uniform(0.98, 1.02), 1),
        }
        diesel_v = out['shunt_diesel']['voltage']
        out['orion_alt'] = {
            'state': orion_state,
            'off_reason': orion_off_reason,
            'input_voltage': round(diesel_v, 2),
            'output_voltage': round(orion_out_v, 2) if orion_active else round(bat48_v, 2),
            'output_current': round(orion_out_a, 2),
            'output_power': round(orion_out_w),
        }
        # ── Orion-Tr Smart 48/12 x2 ──
        if self.house_orion_left > 0:
            self.house_orion_left -= dt
            if self.house_orion_left <= 0:
                if self.house_orion_state == 'Bulk':
                    self.house_orion_state, self.house_orion_left = 'Absorption', random.uniform(*HOUSE_ABS_S)
                else:
                    self.house_orion_state = 'Float'
        elif random.random() < HOUSE_CYCLE_CHANCE * dt:
            self.house_orion_state, self.house_orion_left = 'Bulk', random.uniform(*HOUSE_BULK_S)
        house_out_v = {'Bulk': 13.3, 'Absorption': ORION48_ABS_V}.get(self.house_orion_state, ORION48_FLOAT_V)

        if self.engine_on:
            thr_orion, thr_off = 'Off', 'Remote input'
        elif min(thruster['soc'], diesel['soc']) < 97:
            thr_orion, thr_off = 'Bulk', 'None'
        elif min(thruster['soc'], diesel['soc']) < 99.5:
            thr_orion, thr_off = 'Absorption', 'None'
        else:
            thr_orion, thr_off = 'Float', 'None'
        for name, state, v_out, off in (
                ('orion_house', self.house_orion_state, house_out_v, 'None'),
                ('orion_thruster', thr_orion, out['shunt_thruster']['voltage'], thr_off)):
            out[name] = {
                'state': state,
                'state_code': MPPT_STATE_CODES[state],
                'input_voltage': round(bat48_v + random.uniform(-0.05, 0.05), 2),
                'output_voltage': round(v_out + random.uniform(-0.02, 0.02), 2),
                'off_reason': off,
                'charger_error': 'None',
                'rssi': ORION_RSSI[name] + random.randint(-4, 4),
            }

        # ── 12V house loads ──
        self.fridge_left -= dt
        if self.fridge_left <= 0:
            self.fridge_on = not self.fridge_on
            self.fridge_left = random.uniform(*(FRIDGE_ON_S if self.fridge_on else FRIDGE_OFF_S))
        self.other_a += random.uniform(-0.15, 0.15) * dt
        self.other_a = min(OTHER_A[1], max(OTHER_A[0], self.other_a))
        loads = {
            'fridge': FRIDGE_A * random.uniform(0.95, 1.05) if self.fridge_on else 0.0,
            'laptop': LAPTOP_A * random.uniform(0.9, 1.05),
            'nav': NAV_A * random.uniform(0.97, 1.03),
            'other': self.other_a,
        }
        total = sum(loads.values())
        # Only the total is published -- nothing on the boat meters individual appliances.
        out['house_load'] = {'current': round(total, 2), 'power': round(total * house_out_v)}

        # ── 48V DC loads: watermaker + A/C (total only) ──
        self.wm_left -= dt
        if self.wm_left <= 0:
            self.wm_on = not self.wm_on
            self.wm_left = random.uniform(*(WATERMAKER_ON_S if self.wm_on else WATERMAKER_OFF_S))
        self.ac_left -= dt
        if self.ac_left <= 0:
            self.ac_on = not self.ac_on
            self.ac_left = random.uniform(*(AC_ON_S if self.ac_on else AC_OFF_S))
        w48 = (WATERMAKER_W * random.uniform(0.95, 1.05) if self.wm_on else 0.0) \
            + (AC_W * random.uniform(0.93, 1.05) if self.ac_on else AC_FAN_W)
        out['dc48_load'] = {'power': round(w48), 'current': round(w48 / bat48_v, 2)}

        out['mppt'] = {
            'state': mppt_state,
            'state_code': MPPT_STATE_CODES[mppt_state],
            'pv_voltage': round(pv_v, 1),
            'pv_power': round(pv_w),
            'bat_voltage': round(bat48_v, 2),
            'bat_current': round(out_a, 2),
            'yield_today': round(self.yield_today, 3),
            'yield_total': round(self.yield_total, 1),
            'max_power_today': round(self.max_power_today),
            'error': 'None',
        }

        # ── bimini array on its own MPPT (same bank, same charge state) ──
        if random.random() < 0.02 * dt:
            self.b_shade_target = random.choice([1.0, 1.0, 0.8, 0.5, 0.3])
        self.b_shade += (self.b_shade_target - self.b_shade) * min(1.0, 0.1 * dt)
        b_avail = BIMINI_WP * sun * self.cloud * self.b_shade * random.uniform(0.97, 1.02)
        b_w = min(b_avail * limit, BIMINI_MPPT_MAX_A * bat48_v / 0.97) if b_avail >= 5 else 0.0
        b_state = mppt_state if b_avail >= 5 else 'Off'
        b_a = b_w * 0.97 / bat48_v if b_w > 0 else 0.0
        b_pv_v = 0.0 if sun <= 0 else (BIMINI_PV_VOC * min(1.0, sun * 20) if b_w < 5
                 else BIMINI_PV_VMP + (BIMINI_PV_VOC - BIMINI_PV_VMP) * (1 - limit) * 0.6 + random.uniform(-0.4, 0.4))
        self.b_yield_today += b_w * 0.97 * dt / 3.6e6
        self.b_yield_total += b_w * 0.97 * dt / 3.6e6
        self.b_max_power_today = max(self.b_max_power_today, b_w)
        out['mppt_bimini'] = {
            'state': b_state,
            'state_code': MPPT_STATE_CODES[b_state],
            'pv_voltage': round(b_pv_v, 1),
            'pv_power': round(b_w),
            'bat_voltage': round(bat48_v, 2),
            'bat_current': round(b_a, 2),
            'yield_today': round(self.b_yield_today, 3),
            'yield_total': round(self.b_yield_total, 1),
            'max_power_today': round(self.b_max_power_today),
            'error': 'None',
        }
        return out


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--rate', type=float, default=1.0, help='Publish interval in seconds (default 1 -- thruster bursts and engine starts only last a few seconds)')
    ap.add_argument('--engine', choices=['auto', 'on', 'off'], default='auto', help='auto = runs for a few minutes every so often (default)')
    ap.add_argument('--daylight', action='store_true', help='Force midday sun regardless of the clock')
    args = ap.parse_args()

    secrets = get_secrets()
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=MQTT_CLIENT_ID)
    client.username_pw_set(secrets['MQTT_USER'], secrets['MQTT_PASS'])
    client.connect(MQTT_BROKER, MQTT_PORT, keepalive=30)
    client.loop_start()

    sim = Sim(args.engine, args.daylight)
    print("Simulating: SmartShunt 500A (diesel), SmartShunt 1000A (thruster), MPPT 150/35, Orion 12|48")
    print(f"Engine: {args.engine}   Solar: {'forced daylight' if args.daylight else 'local time of day'}")
    print(f"Publishing to {TOPIC_PREFIX}/<device>/<field> -- Ctrl+C to stop\n")

    last = time.time()
    try:
        while True:
            now = time.time()
            out = sim.step(now - last)
            last = now
            for device, fields in out.items():
                for field, value in fields.items():
                    client.publish(f"{TOPIC_PREFIX}/{device}/{field}", str(value))
            time.sleep(args.rate)
    except KeyboardInterrupt:
        print("\nStopped.")
    finally:
        client.loop_stop()
        client.disconnect()


if __name__ == '__main__':
    main()
