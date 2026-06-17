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
