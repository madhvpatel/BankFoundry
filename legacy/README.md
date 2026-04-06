# Legacy Surfaces

This folder contains the older Streamlit UI and compatibility-only application paths.

- `streamlit_app.py` contains the legacy Streamlit merchant OS surface.
- `app/legacy/copilot/` contains legacy runtime code that is no longer wired to the active ask API.

The original import paths are preserved as compatibility shims under `app/copilot/` and `main.py`.
