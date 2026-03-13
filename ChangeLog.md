 # Changelog

## [2.0.1]

### Added

- Post mode: optional IMG_LINK population from POST_LINK
- Interactive menu when running without --mode

### Updated

- PostQueue parsing updated for TITLE_EN/TITLE_UR/IMG_LINK/POST_LINK layouts
- Post mode console output uses row references to avoid Urdu rendering issues

### Fixed

- Browser setup: fallback to Selenium Manager when local ChromeDriver fails/mismatches
- Post mode: prevent ChromeDriver non-BMP character crashes

## [2.0.0]

### Added
- Modular architecture
- Google Sheets retry system
- Logging with timestamps

### Updated
- Post mode: improved form detection on DamaDam share pages
- Post mode: improved post URL extraction after submit
- Post mode: image defaults set to Never expire + Turn Off Replies = Yes
- Repo hygiene: ignore chromedriver.exe; ensure .env/credentials.json are not committed

### Fixed
- Cookie handling
- Selenium stability

### Known Issues
- Headless detection possible

