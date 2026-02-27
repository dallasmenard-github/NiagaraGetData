# Changelog - Niagara BAS Downloader v2.0

## 2026-02-26

### Added
- **Live progress bar during downloads** — replaces the old silent logger-based
  progress with an in-place console bar showing filled/empty blocks, count,
  percentage, OK/EMPTY/FAIL tallies, and download speed (e.g.
  `[████████░░░░░░░] 2048/6272  32.6%  OK:1890 EMPTY:158  41.2/s`).

### Changed
- `ProgressPrinter` in `niagara_download_engine.py` now writes directly to
  `sys.stdout` with carriage-return updates instead of `logger.info()`.
- Progress update frequency increased (~100 updates per run) for smoother
  visual feedback.
