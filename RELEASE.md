# Release Instructions

This document outlines the steps to release a new version of the Materialize MCP Server to PyPI.

## Prerequisites

1. Install required tools:
```bash
uv pip install build twine
```

2. Ensure you have a PyPI account and have configured your credentials:
```bash
# Create a ~/.pypirc file with your PyPI credentials
cat > ~/.pypirc << EOF
[distutils]
index-servers =
    pypi
    testpypi

[pypi]
repository: https://upload.pypi.org/legacy/
username: your_username
password: your_password

[testpypi]
repository: https://test.pypi.org/legacy/
username: your_username
password: your_password
EOF
```

## Release Steps

1. Update the version number in `pyproject.toml`:
```bash
# Edit pyproject.toml and update the version number
```

2. Build the distribution:
```bash
uv run hatch buildu
```

3. Test the distribution locally:
```bash
uv pip install dist/materialize_mcp_server-<version>-py3-none-any.whl
uv run materialize-mcp --help  # Verify it works
```

4. (Optional) Test the release on TestPyPI:
```bash
uv run twine upload --repository testpypi dist/*
uv pip install --index-url https://test.pypi.org/simple/ materialize-mcp-server
```

5. Create a git tag for the release:
```bash
git tag -a v<version> -m "Release version <version>"
git push origin v<version>
```

6. Publish to PyPI:
```bash
uv run twine upload dist/*
```

7. Verify the release on PyPI:
- Visit https://pypi.org/project/materialize-mcp-server/
- Check that the new version is listed
- Verify the package description and metadata

## Post-Release

1. Update the version number in `pyproject.toml` to the next development version (e.g., from 0.1.0 to 0.1.1.dev0)

2. Commit the version bump:
```bash
git add pyproject.toml
git commit -m "Bump version to <next_version>"
git push
```

## Troubleshooting

If you encounter any issues:

1. Check that all required files are included in the distribution:
```bash
tar -tvf dist/materialize_mcp_server-<version>.tar.gz
```

2. Verify the package metadata:
```bash
uv run twine check dist/*
```

3. If the upload fails, check your PyPI credentials and try again.

4. If the package fails to install, check the build logs and verify all dependencies are correctly specified in `pyproject.toml`. 