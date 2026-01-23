# IDE Setup Recommendations

## VS Code

Install these extensions for the best development experience:

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
