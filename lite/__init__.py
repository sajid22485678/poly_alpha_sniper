"""Poly Alpha Lite V1 -- a SIMPLE, isolated, shadow-only 5-minute Polymarket
trader. Separate from the advanced bot: own config (config_lite.yaml), own
SQLite file (data/poly_alpha_lite.db), own export dir
(agent_readonly/poly_alpha_lite/), own scripts. It never places or cancels
real orders, never reads secrets, and its PnL never touches baseline stats
or live readiness."""
