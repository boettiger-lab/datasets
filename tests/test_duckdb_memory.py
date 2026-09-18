"""
Kubernetes memory quantities are translated into DuckDB's spelling (issue #217).

`SET memory_limit='16Gi'` is a parser error, and the raster workflow used to
write exactly that into the generated merge job's environment. The failure was
expensive in the worst way: it landed after the whole hex fan-out had run, so a
multi-hour build could not be consolidated without hand-editing manifests.

The gate that matters is not the string shape but the round trip — every value
`to_duckdb_memory_limit` produces is handed to a real DuckDB below.
"""

import duckdb
import pytest

from cng_datasets.duckdb_memory import to_duckdb_memory_limit


# Every memory quantity this repo writes into a manifest or takes from a flag.
K8S_QUANTITIES = ["8Gi", "16Gi", "32Gi", "64Gi", "128Gi", "512Mi", "2Ti",
                  "1.5Gi", "8G", "500M", "1000000"]


class TestDuckDBAcceptsWhatWeEmit:
    """The regression gate: DuckDB itself, not a string comparison."""

    @pytest.mark.timeout(10)
    @pytest.mark.parametrize("quantity", K8S_QUANTITIES)
    @pytest.mark.parametrize("fraction", [1.0, 0.85])
    def test_duckdb_parses_the_result(self, quantity, fraction):
        limit = to_duckdb_memory_limit(quantity, fraction)
        con = duckdb.connect()
        con.execute(f"SET memory_limit='{limit}'")

    @pytest.mark.timeout(10)
    def test_the_raw_k8s_spelling_is_what_duckdb_rejects(self):
        """Pins the premise: without translation this is a hard parser error."""
        con = duckdb.connect()
        with pytest.raises(duckdb.ParserException, match="Unknown unit for memory"):
            con.execute("SET memory_limit='16Gi'")

    @pytest.mark.timeout(10)
    @pytest.mark.parametrize("quantity", K8S_QUANTITIES)
    def test_the_limit_duckdb_applies_is_the_one_we_asked_for(self, quantity):
        """Translation must not quietly change the size, only the spelling."""
        con = duckdb.connect()
        con.execute(f"SET memory_limit='{to_duckdb_memory_limit(quantity)}'")
        applied = con.execute(
            "SELECT current_setting('memory_limit')"
        ).fetchone()[0]
        # current_setting reports one decimal place in whichever binary
        # unit it picks, so compare in bytes with a tolerance that covers that
        # rounding rather than comparing the string.
        value, unit = applied.split()
        factor = {"KiB": 2 ** 10, "MiB": 2 ** 20, "GiB": 2 ** 30,
                  "TiB": 2 ** 40, "bytes": 1}[unit]
        assert float(value) * factor == pytest.approx(_bytes(quantity), rel=1e-2)


def _bytes(quantity: str) -> float:
    """Independent reading of a k8s quantity, so the test does not reuse the
    parser it is checking."""
    units = {"Ki": 2 ** 10, "Mi": 2 ** 20, "Gi": 2 ** 30, "Ti": 2 ** 40,
             "K": 10 ** 3, "M": 10 ** 6, "G": 10 ** 9, "T": 10 ** 12}
    for suffix in sorted(units, key=len, reverse=True):
        if quantity.endswith(suffix):
            return float(quantity[:-len(suffix)]) * units[suffix]
    return float(quantity)


class TestSpelling:
    """Readability matters: these land in manifests people read and edit."""

    @pytest.mark.timeout(5)
    @pytest.mark.parametrize("quantity,expected", [
        ("16Gi", "16GiB"),
        ("8Gi", "8GiB"),
        ("512Mi", "512MiB"),
        ("2Ti", "2TiB"),
        ("8G", "8GB"),      # k8s' bare G is 1000^3, and so is DuckDB's GB
        ("500M", "500MB"),
        ("1.5Gi", "1536MiB"),   # stepped down rather than truncated to 1GiB
    ])
    def test_exact_quantities_keep_their_magnitude(self, quantity, expected):
        assert to_duckdb_memory_limit(quantity) == expected

    @pytest.mark.timeout(5)
    @pytest.mark.parametrize("quantity", ["16GiB", "27GiB", "8GB", "512MiB"])
    def test_duckdb_spelling_passes_through(self, quantity):
        """Applied defensively at the consumer, so it must be idempotent."""
        assert to_duckdb_memory_limit(quantity) == quantity

    @pytest.mark.timeout(5)
    @pytest.mark.parametrize("fraction,expected", [(0.85, "27GiB"), (1.0, "32GiB")])
    def test_fraction_scales_the_value(self, fraction, expected):
        assert to_duckdb_memory_limit("32Gi", fraction) == expected

    @pytest.mark.timeout(5)
    def test_a_fraction_never_rounds_up(self):
        """Rounding a memory limit up is how a pod gets OOMKilled."""
        for quantity in K8S_QUANTITIES:
            scaled = to_duckdb_memory_limit(quantity, 0.85)
            assert _duckdb_bytes(scaled) <= _bytes(quantity) * 0.85

    @pytest.mark.timeout(5)
    def test_a_fraction_of_a_small_limit_does_not_collapse_to_zero(self):
        """int(1 * 0.85) is 0, and '0GiB' is not a memory limit."""
        assert _duckdb_bytes(to_duckdb_memory_limit("1Gi", 0.85)) > 0


def _duckdb_bytes(limit: str) -> float:
    units = {"KiB": 2 ** 10, "MiB": 2 ** 20, "GiB": 2 ** 30, "TiB": 2 ** 40,
             "KB": 10 ** 3, "MB": 10 ** 6, "GB": 10 ** 9, "TB": 10 ** 12, "B": 1}
    for suffix in sorted(units, key=len, reverse=True):
        if limit.endswith(suffix):
            return float(limit[:-len(suffix)]) * units[suffix]
    raise AssertionError(f"{limit!r} carries no DuckDB memory unit")


class TestUnparseableInputIsLeftAlone:
    """A value a human set by hand is returned as given rather than guessed at."""

    @pytest.mark.timeout(5)
    @pytest.mark.parametrize("quantity", ["-1", "auto", "", "  "])
    def test_passthrough(self, quantity):
        assert to_duckdb_memory_limit(quantity) == quantity.strip()

    @pytest.mark.timeout(5)
    def test_none_stays_none(self):
        """The consumers fall back to DuckDB's own auto-detection on None."""
        assert to_duckdb_memory_limit(None) is None
