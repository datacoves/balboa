import inspect

import responses
from responses import matchers

from conftest import reload_module

MODULE = "get_feature_pr_ids"


def load():
    return reload_module(MODULE)


def commits_pulls_url(repo, sha):
    return f"https://api.github.com/repos/{repo}/commits/{sha}/pulls"


def pulls_url(repo):
    return f"https://api.github.com/repos/{repo}/pulls"


def pr(number, head_ref, base_ref, merged=False):
    return {
        "number": number,
        "head": {"ref": head_ref},
        "base": {"ref": base_ref},
        "merged_at": "2026-01-01T00:00:00Z" if merged else None,
    }


@responses.activate
def test_merged_feature_prs_into_release_branch_are_returned(base_env, monkeypatch):
    monkeypatch.setenv("GITHUB_SHA", "abc123")
    repo = "datacoves/balboa"

    responses.add(
        responses.GET,
        commits_pulls_url(repo, "abc123"),
        json=[pr(42, "release/x", "main", merged=True)],
        status=200,
    )
    responses.add(
        responses.GET,
        pulls_url(repo),
        json=[
            pr(10, "feature/a", "release/x", merged=True),
            pr(11, "feature/b", "release/x", merged=True),
            # not feature/* -- excluded even though merged into the release branch
            pr(12, "hotfix/c", "release/x", merged=True),
            # feature/* but closed without merging -- excluded
            pr(13, "feature/d", "release/x", merged=False),
        ],
        status=200,
        match=[matchers.query_param_matcher(
            {"state": "closed", "base": "release/x", "per_page": "100", "page": "1"}
        )],
    )

    mod = load()
    mod.run()

    assert base_env("feature_pr_ids") == "10,11"


@responses.activate
def test_pagination_combines_results_across_pages(base_env, monkeypatch):
    monkeypatch.setenv("GITHUB_SHA", "abc123")
    repo = "datacoves/balboa"

    responses.add(
        responses.GET,
        commits_pulls_url(repo, "abc123"),
        json=[pr(1, "release/x", "main", merged=True)],
        status=200,
    )

    page1 = [pr(n, f"feature/{n}", "release/x", merged=True) for n in range(1, 101)]
    page2 = [pr(101, "feature/101", "release/x", merged=True)]

    responses.add(
        responses.GET,
        pulls_url(repo),
        json=page1,
        status=200,
        match=[matchers.query_param_matcher(
            {"state": "closed", "base": "release/x", "per_page": "100", "page": "1"}
        )],
    )
    responses.add(
        responses.GET,
        pulls_url(repo),
        json=page2,
        status=200,
        match=[matchers.query_param_matcher(
            {"state": "closed", "base": "release/x", "per_page": "100", "page": "2"}
        )],
    )

    mod = load()
    mod.run()

    ids = base_env("feature_pr_ids").split(",")
    assert len(ids) == 101
    assert ids[0] == "1"
    assert ids[-1] == "101"
    assert len(responses.calls) == 3  # commit lookup + 2 pages


@responses.activate
def test_no_pr_found_for_commit_writes_empty_output(base_env, monkeypatch):
    monkeypatch.setenv("GITHUB_SHA", "abc123")
    repo = "datacoves/balboa"

    responses.add(
        responses.GET,
        commits_pulls_url(repo, "abc123"),
        json=[],
        status=200,
    )

    mod = load()
    mod.run()

    assert base_env("feature_pr_ids") == ""


@responses.activate
def test_non_release_source_branch_is_skipped(base_env, monkeypatch):
    monkeypatch.setenv("GITHUB_SHA", "abc123")
    repo = "datacoves/balboa"

    # hotfix/* merged straight to main -- targeted cleanup must not touch it
    responses.add(
        responses.GET,
        commits_pulls_url(repo, "abc123"),
        json=[pr(5, "hotfix/urgent-fix", "main", merged=True)],
        status=200,
    )

    mod = load()
    mod.run()

    assert base_env("feature_pr_ids") == ""
    # only the commit lookup should have been called -- no /pulls listing
    assert len(responses.calls) == 1


@responses.activate
def test_api_error_never_raises_and_writes_empty_output(base_env, monkeypatch):
    monkeypatch.setenv("GITHUB_SHA", "abc123")
    repo = "datacoves/balboa"

    responses.add(
        responses.GET,
        commits_pulls_url(repo, "abc123"),
        status=500,
    )

    mod = load()
    mod.run()  # must not raise

    assert base_env("feature_pr_ids") == ""


def test_script_never_reads_commit_messages():
    """Structural guarantee: the targeted-cleanup logic is commit-message-agnostic
    (works for squash merges and custom messages) because it never parses commit
    text or shells out to git at all."""
    mod = load()
    source = inspect.getsource(mod)
    assert "subprocess" not in source
    assert "git log" not in source
    assert "pretty=%B" not in source
