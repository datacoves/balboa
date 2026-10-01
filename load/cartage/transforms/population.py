"""Reshape the wide US population CSV (one column per year, numbers as "4,785,437") into one document per state."""


def by_year(record: dict) -> dict:
    years = sorted(k for k in record if str(k).isdigit())
    populations = [{"year": int(y), "population": int(str(record[y]).replace(",", ""))} for y in years]
    first, last = populations[0]["population"], populations[-1]["population"]
    return {
        "state": record["states"],
        "populations": populations,
        "growth_pct": round((last - first) / first * 100, 2),
    }


def at_least(record: dict, population: int) -> bool:
    """Keep states whose latest population is at least `population`."""
    return record["populations"][-1]["population"] >= population
