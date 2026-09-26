# Database tuning & rollup: install / revert

Everything here needs `sudo` (the assistant that wrote it can't run these).
Apply in this order — each step is independent and safe to do alone.

## 1. MariaDB buffer pool + commit flushing (biggest single win)

```bash
sudo cp ~/dashboard-dev/ops/99-boat-tuning.cnf /etc/mysql/mariadb.conf.d/99-boat-tuning.cnf
# apply live, no restart (both variables are dynamic in 10.11):
sudo mysql -e "SET GLOBAL innodb_buffer_pool_size = 805306368; SET GLOBAL innodb_flush_log_at_trx_commit = 2;"
mysql -u mikemc -p boat_monitoring -e "SELECT @@innodb_buffer_pool_size/1024/1024 AS pool_mb, @@innodb_flush_log_at_trx_commit"
```
The pool resizes online in the background. The drop-in makes it survive a restart.
Revert: `sudo rm /etc/mysql/mariadb.conf.d/99-boat-tuning.cnf`, then set the two values back
(`134217728` and `1`) the same way.

## 2. Logger write reduction (`mqtt_logger.py` + `logger_policy.py`)

`~/python/mqtt_logger.py` is a symlink into this repo, and `mqtt-logger.service` runs it —
so the file edits are already "live" for the **next restart**; the running process is unchanged
until you restart it. `logger_policy.py` must stay next to `mqtt_logger.py`.

```bash
python3 -m unittest test_logger_policy          # from ~/dashboard-dev; should say OK
sudo systemctl restart mqtt-logger.service
journalctl -u mqtt-logger.service -n 20 --no-pager   # expect "MQTT connected" + "Subscribed"
```
Revert: `git checkout mqtt_logger.py` (in ~/dashboard-dev) and restart the service.
Behavior knobs are constants at the top of `mqtt_logger.py` (`NAV_IS_SIMULATED`,
`READING_MIN_INTERVAL_S`, `READING_HEARTBEAT_S`, `TOUCH_INTERVAL_S`).

**When the CAN HAT goes live**, set `NAV_IS_SIMULATED = False` to resume logging real
position/depth/heading/autopilot history into `mqtt_readings`, then restart the logger.

## 3. Trend rollup (`mqtt_rollup.py`)

```bash
sudo cp ~/dashboard-dev/ops/mqtt-rollup.service ~/dashboard-dev/ops/mqtt-rollup.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now mqtt-rollup.timer
systemctl list-timers mqtt-rollup.timer
journalctl -u mqtt-rollup.service -n 20 --no-pager
python3 ~/dashboard-dev/mqtt_rollup.py --status   # coverage window + row counts
```
The first run backfills 31 days (CPU-bound on the DB, ~3 min per day of data). Until the
rollup covers a chart's whole range, the dashboard transparently uses the old raw query,
so charts are always correct — just slow until the backfill finishes.
The dashboard read path lives in `dashboard_api.py` (`_query_bucketed_from_rollup`); it
only takes effect on prod once `dashboard_api.py` is deployed.
Revert: disabling the timer alone is NOT enough — the rollup would freeze while the dashboard
keeps reading the ever-growing un-rolled-up tail raw, so charts get slower each day. Do both:

```bash
sudo systemctl disable --now mqtt-rollup.timer
mysql -u mikemc -p boat_monitoring -e "DROP TABLE mqtt_readings_1m, mqtt_rollup_state"
```
With the state table gone the dashboard falls back to the original raw query immediately.
