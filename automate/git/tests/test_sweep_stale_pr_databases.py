import urllib.parse
from datetime import datetime, timedelta, timezone

import responses
from responses import matchers

from conftest import reload_module

MODULE = "sweep_stale_pr_databases"


def load():
    return reload_module(MODULE)


def pulls_url(repo):
    return f"https://api.github.com/repos/{repo}/pulls"


def pull_url(repo, pr_id):
    return f"https://api.github.com/repos/{repo}/pulls/{pr_id}"


def branches_url(repo, branch):
    # branch_exists() quotes the branch name (slashes included) for the path,
    # since the GitHub API needs "release%2Fx", not "release/x" as extra segments
    return f"https://api.github.com/repos/{repo}/branches/{urllib.parse.quote(branch, safe='')}"


def iso(days_ago):
    return (datetime.now(timezone.utc) - timedelta(days=days_ago)).isoformat()


def db_list_entry(name, created_days_ago, last_query_days_ago=None):
    created = iso(created_days_ago)
    last_query = iso(last_query_days_ago) if last_query_days_ago is not None else ""
    return f"{name},{created},{last_query}"


REPO = "datacoves/balboa"


def set_db_list(monkeypatch, *entries):
    monkeypatch.setenv("PR_DATABASE_LIST", "|".join(entries))


@responses.activate
def test_abandoned_pr_is_dropped(base_env, monkeypatch):
    set_db_list(monkeypatch, db_list_entry("BALBOA_PR_1", 5))
    responses.add(responses.GET, pull_url(REPO, "1"), json={
        "state": "closed", "merged": False, "base": {"ref": "release/x"},
    }, status=200)

    mod = load()
    mod.run()

    assert base_env("sweep_pr_ids") == "1"


@responses.activate
def test_deleted_pr_404_is_dropped(base_env, monkeypatch):
    set_db_list(monkeypatch, db_list_entry("BALBOA_PR_2", 5))
    responses.add(responses.GET, pull_url(REPO, "2"), status=404)

    mod = load()
    mod.run()

    assert base_env("sweep_pr_ids") == "2"


@responses.activate
def test_open_pr_is_kept(base_env, monkeypatch):
    set_db_list(monkeypatch, db_list_entry("BALBOA_PR_3", 5))
    responses.add(responses.GET, pull_url(REPO, "3"), json={
        "state": "open", "merged": False, "base": {"ref": "release/x"},
    }, status=200)

    mod = load()
    mod.run()

    assert base_env("sweep_pr_ids") == ""


@responses.activate
def test_merged_into_non_release_branch_is_dropped(base_env, monkeypatch):
    set_db_list(monkeypatch, db_list_entry("BALBOA_PR_4", 5))
    responses.add(responses.GET, pull_url(REPO, "4"), json={
        "state": "closed", "merged": True, "base": {"ref": "main"},
    }, status=200)

    mod = load()
    mod.run()

    assert base_env("sweep_pr_ids") == "4"


@responses.activate
def test_merged_into_release_with_open_pr_to_main_is_kept(base_env, monkeypatch):
    set_db_list(monkeypatch, db_list_entry("BALBOA_PR_5", 5))
    responses.add(responses.GET, pull_url(REPO, "5"), json={
        "state": "closed", "merged": True, "base": {"ref": "release/x"},
    }, status=200)
    responses.add(
        responses.GET, pulls_url(REPO), json=[{"state": "open", "merged_at": None}],
        status=200,
        match=[matchers.query_param_matcher(
            {"state": "all", "head": "datacoves:release/x", "base": "main", "per_page": "100"}
        )],
    )

    mod = load()
    mod.run()

    assert base_env("sweep_pr_ids") == ""


@responses.activate
def test_merged_into_release_with_merged_pr_to_main_is_dropped(base_env, monkeypatch):
    set_db_list(monkeypatch, db_list_entry("BALBOA_PR_6", 5))
    responses.add(responses.GET, pull_url(REPO, "6"), json={
        "state": "closed", "merged": True, "base": {"ref": "release/x"},
    }, status=200)
    responses.add(
        responses.GET, pulls_url(REPO),
        json=[{"state": "closed", "merged_at": "2026-01-01T00:00:00Z"}],
        status=200,
        match=[matchers.query_param_matcher(
            {"state": "all", "head": "datacoves:release/x", "base": "main", "per_page": "100"}
        )],
    )

    mod = load()
    mod.run()

    assert base_env("sweep_pr_ids") == "6"


@responses.activate
def test_merged_into_release_with_abandoned_pr_to_main_is_dropped(base_env, monkeypatch):
    set_db_list(monkeypatch, db_list_entry("BALBOA_PR_7", 5))
    responses.add(responses.GET, pull_url(REPO, "7"), json={
        "state": "closed", "merged": True, "base": {"ref": "release/x"},
    }, status=200)
    responses.add(
        responses.GET, pulls_url(REPO),
        json=[{"state": "closed", "merged_at": None}],
        status=200,
        match=[matchers.query_param_matcher(
            {"state": "all", "head": "datacoves:release/x", "base": "main", "per_page": "100"}
        )],
    )

    mod = load()
    mod.run()

    assert base_env("sweep_pr_ids") == "7"


@responses.activate
def test_release_branch_no_pr_to_main_young_is_kept(base_env, monkeypatch):
    # age (5d) <= PR_MAX_AGE_DAYS (15) -> keep regardless of activity
    set_db_list(monkeypatch, db_list_entry("BALBOA_PR_8", 5))
    responses.add(responses.GET, pull_url(REPO, "8"), json={
        "state": "closed", "merged": True, "base": {"ref": "release/x"},
    }, status=200)
    responses.add(
        responses.GET, pulls_url(REPO), json=[],
        status=200,
        match=[matchers.query_param_matcher(
            {"state": "all", "head": "datacoves:release/x", "base": "main", "per_page": "100"}
        )],
    )
    responses.add(responses.GET, branches_url(REPO, "release/x"), status=200)

    mod = load()
    mod.run()

    assert base_env("sweep_pr_ids") == ""


@responses.activate
def test_release_branch_no_pr_to_main_old_but_recently_queried_is_kept(base_env, monkeypatch):
    # age (20d) > max (15) but queried 3 days ago (<= idle max 15) -> keep
    set_db_list(monkeypatch, db_list_entry("BALBOA_PR_9", 20, last_query_days_ago=3))
    responses.add(responses.GET, pull_url(REPO, "9"), json={
        "state": "closed", "merged": True, "base": {"ref": "release/x"},
    }, status=200)
    responses.add(
        responses.GET, pulls_url(REPO), json=[],
        status=200,
        match=[matchers.query_param_matcher(
            {"state": "all", "head": "datacoves:release/x", "base": "main", "per_page": "100"}
        )],
    )
    responses.add(responses.GET, branches_url(REPO, "release/x"), status=200)

    mod = load()
    mod.run()

    assert base_env("sweep_pr_ids") == ""


@responses.activate
def test_release_branch_no_pr_to_main_old_and_idle_is_dropped(base_env, monkeypatch):
    # age (20d) > max (15), no recent queries -> drop
    set_db_list(monkeypatch, db_list_entry("BALBOA_PR_10", 20))
    responses.add(responses.GET, pull_url(REPO, "10"), json={
        "state": "closed", "merged": True, "base": {"ref": "release/x"},
    }, status=200)
    responses.add(
        responses.GET, pulls_url(REPO), json=[],
        status=200,
        match=[matchers.query_param_matcher(
            {"state": "all", "head": "datacoves:release/x", "base": "main", "per_page": "100"}
        )],
    )
    responses.add(responses.GET, branches_url(REPO, "release/x"), status=200)

    mod = load()
    mod.run()

    assert base_env("sweep_pr_ids") == "10"


@responses.activate
def test_release_branch_no_pr_to_main_past_hard_max_is_dropped_even_if_active(base_env, monkeypatch):
    # age (50d) > hard max (45) -> drop even though queried yesterday
    set_db_list(monkeypatch, db_list_entry("BALBOA_PR_11", 50, last_query_days_ago=1))
    responses.add(responses.GET, pull_url(REPO, "11"), json={
        "state": "closed", "merged": True, "base": {"ref": "release/x"},
    }, status=200)
    responses.add(
        responses.GET, pulls_url(REPO), json=[],
        status=200,
        match=[matchers.query_param_matcher(
            {"state": "all", "head": "datacoves:release/x", "base": "main", "per_page": "100"}
        )],
    )
    responses.add(responses.GET, branches_url(REPO, "release/x"), status=200)

    mod = load()
    mod.run()

    assert base_env("sweep_pr_ids") == "11"


@responses.activate
def test_release_branch_deleted_no_pr_to_main_is_dropped(base_env, monkeypatch):
    set_db_list(monkeypatch, db_list_entry("BALBOA_PR_12", 5))
    responses.add(responses.GET, pull_url(REPO, "12"), json={
        "state": "closed", "merged": True, "base": {"ref": "release/x"},
    }, status=200)
    responses.add(
        responses.GET, pulls_url(REPO), json=[],
        status=200,
        match=[matchers.query_param_matcher(
            {"state": "all", "head": "datacoves:release/x", "base": "main", "per_page": "100"}
        )],
    )
    responses.add(responses.GET, branches_url(REPO, "release/x"), status=404)

    mod = load()
    mod.run()

    assert base_env("sweep_pr_ids") == "12"


@responses.activate
def test_unknown_api_error_is_kept_for_now(base_env, monkeypatch):
    set_db_list(monkeypatch, db_list_entry("BALBOA_PR_13", 5))
    responses.add(responses.GET, pull_url(REPO, "13"), status=503)

    mod = load()
    mod.run()

    assert base_env("sweep_pr_ids") == ""


@responses.activate
def test_release_state_is_cached_across_databases_sharing_a_release(base_env, monkeypatch):
    set_db_list(
        monkeypatch,
        db_list_entry("BALBOA_PR_20", 5),
        db_list_entry("BALBOA_PR_21", 5),
    )
    responses.add(responses.GET, pull_url(REPO, "20"), json={
        "state": "closed", "merged": True, "base": {"ref": "release/shared"},
    }, status=200)
    responses.add(responses.GET, pull_url(REPO, "21"), json={
        "state": "closed", "merged": True, "base": {"ref": "release/shared"},
    }, status=200)
    responses.add(
        responses.GET, pulls_url(REPO),
        json=[{"state": "closed", "merged_at": "2026-01-01T00:00:00Z"}],
        status=200,
        match=[matchers.query_param_matcher(
            {"state": "all", "head": "datacoves:release/shared", "base": "main", "per_page": "100"}
        )],
    )

    mod = load()
    mod.run()

    ids = set(base_env("sweep_pr_ids").split(","))
    assert ids == {"20", "21"}
    # exactly one call to the head=...:release/shared&base=main lookup, despite two databases
    status_calls = [c for c in responses.calls if "head=datacoves%3Arelease%2Fshared" in c.request.url]
    assert len(status_calls) == 1


def test_empty_database_list_writes_empty_output(base_env, monkeypatch):
    monkeypatch.setenv("PR_DATABASE_LIST", "")

    mod = load()
    mod.run()

    assert base_env("sweep_pr_ids") == ""


def test_malformed_database_list_entries_are_ignored(base_env, monkeypatch):
    # no "_PR_<digits>" suffix anywhere -- nothing parses, nothing to sweep
    monkeypatch.setenv("PR_DATABASE_LIST", "NOT_A_PR_DB,2026-01-01T00:00:00+00:00,")

    mod = load()
    mod.run()

    assert base_env("sweep_pr_ids") == ""


def test_invalid_max_age_days_aborts_sweep_without_dropping_anything(base_env, monkeypatch):
    set_db_list(monkeypatch, db_list_entry("BALBOA_PR_30", 999))  # would obviously be dropped if parsed
    monkeypatch.setenv("PR_MAX_AGE_DAYS", "not-a-number")

    mod = load()
    mod.run()  # must not raise despite the bad env var

    assert base_env("sweep_pr_ids") == ""
