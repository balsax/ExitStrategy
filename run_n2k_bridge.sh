#!/bin/bash
# Real invocation for n2k_mqtt_bridge.py against the physical CAN bus -- see
# the module docstring in n2k_mqtt_bridge.py for why this exact pipeline (not
# `candump -L can0 | analyzer -json` as ~/claude.md's older setup notes say)
# is what actually works. Installed as the n2k-mqtt-bridge.service unit.
set -uo pipefail
candump -L can0 | candump2analyzer | analyzer -json -debugdata | python3 /home/mikemc/n2k_mqtt_bridge.py
