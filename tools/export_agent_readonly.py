"""Write sanitized, read-only status/report files for a future Hermes Agent
/ Obsidian integration.

Run (from the project root, no PYTHONPATH setup needed):
  .venv\\Scripts\\python.exe tools\\export_agent_readonly.py
  (or: scripts\\export_agent_readonly.bat)
Optional: --output-dir <path> to override config.yaml's agent_export.output_dir

Read-only: never touches .env, config.yaml, or the bot's order/execution
path. Safe to run at any time, including while the bot is live-running (it
opens the DB read-only, same as the dashboard).
"""
from __future__ import annotations

import sys
from pathlib import Path

# make poly_alpha_sniper importable when run directly (matches main.py)
_PARENT = str(Path(__file__).resolve().parent.parent.parent)
if _PARENT not in sys.path:
    sys.path.insert(0, _PARENT)


def main() -> None:
    from poly_alpha_sniper.core.config_loader import load_config
    from poly_alpha_sniper.reporting.agent_export import write_exports

    output_dir = None
    args = sys.argv[1:]
    if "--output-dir" in args:
        idx = args.index("--output-dir")
        if idx + 1 < len(args):
            output_dir = args[idx + 1]

    cfg = load_config()
    if not cfg.agent_export.enabled:
        print("agent_export.enabled is false in config.yaml — nothing written.")
        return

    result = write_exports(cfg, output_dir=output_dir)
    print(f"wrote {len(result['files_written'])} file(s) to {result['output_dir']}:")
    for path in result["files_written"]:
        print(f"  {path}")
    if not result["has_data"]:
        print("NOTE: no bot database found/readable — exports reflect an empty/demo state.")


if __name__ == "__main__":
    main()
