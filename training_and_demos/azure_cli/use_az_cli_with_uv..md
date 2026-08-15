# Azure CLI Setup (via uv)

The `azure-cli` package requires pre-release dependencies and doesn't work with `uvx` ephemeral environments. Use `uv tool install` for a persistent install.

## Install

```sh
uv tool install 'azure-cli>=2.60' --force --prerelease=allow
```

## Login

```sh
az login --use-device-code
```

Follow the prompt to open the URL and enter the device code to authenticate.

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| `No module named 'pkg_resources'` | Missing `setuptools` in ephemeral env | Use `uv tool install` instead of `uvx` |
| `No module named azure.cli` | Resolved ancient version (2.0.67) | Pin `>=2.60` with `--force` |
| Pre-release resolution failure | azure-cli depends on beta sub-packages | Add `--prerelease=allow` |
