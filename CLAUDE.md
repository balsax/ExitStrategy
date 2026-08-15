# Project rules

- This is a DEV COPY of a live Flask dashboard for the sailboat "Exit Strategy."
- Production files:
  - Flask API: /home/mikemc/dashboard_api.py (systemd service: dashboard-api.service, port 5001)
  - Frontend: /var/www/dashboard/index.html (served by Nginx on port 8080)
- NEVER modify, restart, or touch anything under /home/mikemc/dashboard_api.py,
  /var/www/dashboard, or the dashboard-api systemd service unless I explicitly
  say "update production" or "deploy to production."
- All edits stay in this dev directory (~/dashboard-dev) only.
- Run the dev Flask app on a different port than production (use 5003) so it
  doesn't conflict with the live service.
