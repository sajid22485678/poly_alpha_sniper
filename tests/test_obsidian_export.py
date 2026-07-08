"""Tests for the optional Obsidian vault export (reporting/obsidian_export.py).
Disabled by default, never overwrites without explicit config, date-stamped
filenames, does not require Obsidian to be installed."""
from pathlib import Path

from poly_alpha_sniper.core.config_loader import load_config
from poly_alpha_sniper.reporting.obsidian_export import (
    backup_before_overwrite, copy_note_to_vault, dated_note_filename)

NOW_MS = 1_752_000_000_000  # 2025-07-08T18:40:00Z


def _cfg(vault_dir, enabled=True, overwrite=False):
    cfg = load_config()
    cfg.obsidian.enabled = enabled
    cfg.obsidian.vault_notes_dir = str(vault_dir)
    cfg.obsidian.overwrite_existing = overwrite
    return cfg


def test_dated_note_filename_format():
    name = dated_note_filename(NOW_MS)
    assert name.startswith("poly_alpha_sniper_daily_")
    assert name.endswith(".md")
    # YYYY-MM-DD present
    import re
    assert re.search(r"\d{4}-\d{2}-\d{2}\.md$", name)


def test_disabled_by_default_does_not_copy(tmp_path):
    src = tmp_path / "note.md"
    src.write_text("# note", encoding="utf-8")
    vault = tmp_path / "vault"
    cfg = _cfg(vault, enabled=False)
    result = copy_note_to_vault(str(src), cfg, now_ms=NOW_MS)
    assert result["copied"] is False
    assert not vault.exists()  # never even creates the vault dir when disabled


def test_enabled_creates_vault_dir_and_date_stamped_copy(tmp_path):
    src = tmp_path / "note.md"
    src.write_text("# hello vault", encoding="utf-8")
    vault = tmp_path / "vault" / "04_Hermes Reports"
    cfg = _cfg(vault, enabled=True)

    result = copy_note_to_vault(str(src), cfg, now_ms=NOW_MS)

    assert result["copied"] is True
    dest = Path(result["dest"])
    assert dest.exists()
    assert dest.parent == vault
    assert dest.name == dated_note_filename(NOW_MS)
    assert dest.read_text(encoding="utf-8") == "# hello vault"


def test_missing_source_note_is_reported_not_raised(tmp_path):
    cfg = _cfg(tmp_path / "vault", enabled=True)
    result = copy_note_to_vault(str(tmp_path / "missing.md"), cfg, now_ms=NOW_MS)
    assert result["copied"] is False
    assert "not found" in result["reason"]


def test_does_not_overwrite_existing_note_by_default(tmp_path):
    src = tmp_path / "note.md"
    src.write_text("# v2", encoding="utf-8")
    vault = tmp_path / "vault"
    vault.mkdir()
    existing = vault / dated_note_filename(NOW_MS)
    existing.write_text("# v1 -- do not touch", encoding="utf-8")
    cfg = _cfg(vault, enabled=True, overwrite=False)

    result = copy_note_to_vault(str(src), cfg, now_ms=NOW_MS)

    assert result["copied"] is False
    assert existing.read_text(encoding="utf-8") == "# v1 -- do not touch"


def test_overwrites_when_explicitly_configured(tmp_path):
    src = tmp_path / "note.md"
    src.write_text("# v2", encoding="utf-8")
    vault = tmp_path / "vault"
    vault.mkdir()
    existing = vault / dated_note_filename(NOW_MS)
    existing.write_text("# v1", encoding="utf-8")
    cfg = _cfg(vault, enabled=True, overwrite=True)

    result = copy_note_to_vault(str(src), cfg, now_ms=NOW_MS)

    assert result["copied"] is True
    assert existing.read_text(encoding="utf-8") == "# v2"


def test_overwrite_backs_up_the_prior_note_first(tmp_path):
    """'allowed to overwrite' must never mean 'allowed to destroy without a
    copy' -- even when overwrite_existing=True, the old note is preserved."""
    src = tmp_path / "note.md"
    src.write_text("# v2", encoding="utf-8")
    vault = tmp_path / "vault"
    vault.mkdir()
    existing = vault / dated_note_filename(NOW_MS)
    existing.write_text("# v1 -- original content", encoding="utf-8")
    cfg = _cfg(vault, enabled=True, overwrite=True)

    result = copy_note_to_vault(str(src), cfg, now_ms=NOW_MS)

    assert result["copied"] is True
    assert result["backup_path"] is not None
    backup = Path(result["backup_path"])
    assert backup.exists()
    assert backup.read_text(encoding="utf-8") == "# v1 -- original content"


def test_backup_before_overwrite_returns_none_when_nothing_to_back_up(tmp_path):
    result = backup_before_overwrite(str(tmp_path / "does_not_exist.md"))
    assert result is None


def test_backup_before_overwrite_creates_timestamped_copy(tmp_path):
    original = tmp_path / "Poly Alpha Home.md"
    original.write_text("# Home\noriginal content", encoding="utf-8")

    backup_path = backup_before_overwrite(str(original), now_ms=NOW_MS)

    assert backup_path is not None
    backup = Path(backup_path)
    assert backup.exists()
    assert backup != original
    assert backup.read_text(encoding="utf-8") == "# Home\noriginal content"
    assert "backup" in backup.name
    assert "2025-07-08" in backup.name or "2025" in backup.name  # date-stamped


def test_backup_before_overwrite_uses_separate_backup_dir_when_given(tmp_path):
    original = tmp_path / "note.md"
    original.write_text("content", encoding="utf-8")
    archive = tmp_path / "99_Poly_Archive"

    backup_path = backup_before_overwrite(str(original), backup_dir=str(archive), now_ms=NOW_MS)

    assert Path(backup_path).parent == archive
    assert archive.exists()


def test_backup_before_overwrite_does_not_touch_original(tmp_path):
    original = tmp_path / "note.md"
    original.write_text("untouched", encoding="utf-8")
    backup_before_overwrite(str(original), now_ms=NOW_MS)
    assert original.read_text(encoding="utf-8") == "untouched"


def test_config_defaults_are_safe():
    """Loading config.yaml as-shipped: obsidian must be disabled and never
    overwrite -- opt-in only."""
    cfg = load_config()
    assert cfg.obsidian.enabled is False
    assert cfg.obsidian.overwrite_existing is False
    assert cfg.agent_export.enabled is True  # the read-only export itself is safe-on
