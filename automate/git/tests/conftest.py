import importlib
import sys
from pathlib import Path

import pytest

GIT_DIR = Path(__file__).resolve().parent.parent
if str(GIT_DIR) not in sys.path:
    sys.path.insert(0, str(GIT_DIR))


@pytest.fixture
def github_output(tmp_path, monkeypatch):
    """Point GITHUB_OUTPUT at a throwaway file and return a helper to read it back."""
    output_file = tmp_path / "github_output.txt"
    output_file.write_text("")
    monkeypatch.setenv("GITHUB_OUTPUT", str(output_file))

    def read(name):
        """Return the last value written for `name`, or None if never written."""
        value = None
        for line in output_file.read_text().splitlines():
            key, _, val = line.partition("=")
            if key == name:
                value = val
        return value

    return read


@pytest.fixture
def base_env(monkeypatch, github_output):
    monkeypatch.setenv("GITHUB_TOKEN", "test-token")
    monkeypatch.setenv("GITHUB_REPOSITORY", "datacoves/balboa")
    return github_output


def reload_module(name):
    """Import (or re-import) a module fresh so module-level state never leaks between tests."""
    if name in sys.modules:
        return importlib.reload(sys.modules[name])
    return importlib.import_module(name)
