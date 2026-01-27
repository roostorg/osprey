# Development Workflow

## Branch Management

- **Branch naming convention**: Use `github_username/description` format (e.g., `caidanw/feature-auth`, `caidanw/fix-database-timeout`)
- **Base branch**: Always branch from `main`
- **Create new branch**: `git checkout -b username/feature-name`

## Code Quality Standards

### Automated Checks

Every commit automatically runs:

1. **Trailing whitespace removal**
2. **End-of-file fixing**
3. **YAML/JSON/TOML validation**
4. **Ruff linting and formatting**

### Manual Checks

Before pushing, run:

```bash
# Comprehensive linting check
uv run ruff check

# Format all code
uv run ruff format

# Type checking (on specific files/modules)
uv run mypy osprey_worker/src/osprey_worker/lib
# Or you can type check every module (this will happen in CI)
uv run mypy .

# Run all pre-commit hooks
uv run pre-commit run --all-files
```

## Commit Standards

Follow [Conventional Commits](https://www.conventionalcommits.org/) format:

```
feat: add user authentication system
fix: resolve database connection timeout
docs: update API documentation
refactor: simplify rule evaluation logic
```

**Examples:**

- `feat:` - New features
- `fix:` - Bug fixes
- `docs:` - Documentation changes
- `refactor:` - Code refactoring
- `test:` - Adding or updating tests
- `chore:` - Maintenance tasks

## Making Changes

1. **Create a new branch:**

   ```bash
   git checkout -b username/feature-name
   ```

2. **Make your changes**

3. **Run quality checks:**

   ```bash
   uv run ruff check --fix
   uv run ruff format
   ```

4. **Test your changes** (if tests exist)

5. **Commit your changes:**

   ```bash
   git add .
   git commit -m "feat: descriptive commit message"
   ```

   Pre-commit hooks will run automatically and may fix formatting issues.

6. **Push your branch:**

   ```bash
   git push origin username/feature-name
   ```
