# GitHub Copilot Instructions

## General Guidelines

- Prefer to return early from functions to reduce nesting and improve code readability.

## Commit Message Format

All commits must follow the <a href="https://www.conventionalcommits.org/">Conventional Commits</a> format using the Angular preset.

For detailed guidelines on commit types, scopes, and formatting rules, see the <a href="https://github.com/agrc/release-composite-action/blob/main/README.md#commits">release-composite-action README</a>.

## Code Style and Conventions

### Python Style
- Line length: 120 characters (configured in ruff)
- Indentation: 4 spaces for Python files
- Use type hints for all new work
- Follow PEP 8 conventions
- Use pylint disable comments sparingly and only when necessary (e.g., `# pylint: disable=invalid-name`)

### Documentation
- Use docstrings for all classes and public methods
- Follow NumPy/SciPy docstring format with sections:
  - Brief description
  - `Attributes` for class attributes
  - `Parameters` for method parameters
  - `Returns` for return values
  - `Methods` for public methods in class docstrings

## Testing Guidelines

- Mock external services
- Test both success and failure paths
- Verify warning messages for invalid configurations

## Code Quality

- Run `ruff` for linting before committing
- Maintain test coverage (tracked via codecov)
- Follow existing patterns in the codebase
- Keep methods focused and single-purpose
- Use static methods when methods don't need instance state
