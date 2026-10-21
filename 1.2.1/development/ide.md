# IDE Setup

Use the IDE of your choice to work on Osprey.

## VS Code

If using VS Code, we recommend you install these extensions for the best development experience:

- **Python** (Microsoft)
- **Ruff** (Astral Software)
- **MyPy Type Checker** (Microsoft)

**Settings** (add to `.vscode/settings.json`):

```json
{
  "python.defaultInterpreterPath": ".venv/bin/python",
  "ruff.enable": true,
  "ruff.organizeImports": true,
  "python.analysis.typeCheckingMode": "basic"
}
```

## SML files

There's no dedicated editor tooling for SML yet, but since SML is a legal subset of Python syntax, associating `.sml` files with Python gets you reasonable highlighting. In VS Code:

```json
"files.associations": {
  "*.sml": "python"
}
```
