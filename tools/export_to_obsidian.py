"""Copy the latest obsidian_daily_note.md into the configured Obsidian vault
folder. Disabled by default -- requires obsidian.enabled=true in
config.yaml. Does not require Obsidian to be installed; this is a plain file
copy. Run the full exporter first (export_agent_readonly.py) so the note
exists and is current, or just run this repeatedly -- copy_note_to_vault()
is also invoked automatically by write_exports() when obsidian.enabled=true.

Run (from the project root, no PYTHONPATH setup needed):
  .venv\\Scripts\\python.exe tools\\export_to_obsidian.py
  (or: scripts\\export_to_obsidian.bat)
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
    from poly_alpha_sniper.reporting.obsidian_export import copy_note_to_vault

    cfg = load_config()
    if not cfg.obsidian.enabled:
        print("obsidian.enabled is false in config.yaml - nothing to do. "
             "Set obsidian.enabled: true to opt in.")
        return

    note_path = Path(cfg.agent_export.output_dir) / "obsidian_daily_note.md"
    result = copy_note_to_vault(str(note_path), cfg)
    if result["copied"]:
        print(f"copied note to: {result['dest']}")
    else:
        print(f"not copied: {result['reason']}")


if __name__ == "__main__":
    main()
