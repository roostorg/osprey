# Troubleshooting

## Common Issues

### "uv: command not found"

**Solution**: Install uv using the installation script or pip:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
# Then restart your terminal
```

### Pre-commit hooks failing

**Solution**: Run hooks manually to see detailed errors:

```bash
uv run pre-commit run --all-files
```

### MyPy errors on protobuf files

**Solution**: Protobuf generated files are excluded in configuration. If you see errors, check that files match the exclusion patterns in `pyproject.toml`.

### Import errors during type checking

**Solution**: Ensure all dependencies are installed:

```bash
uv sync
```
