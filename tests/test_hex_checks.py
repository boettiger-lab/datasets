"""Tests for post-build hex invariants (issue #102)."""

import os
import tempfile

import duckdb
import pytest

from cng_datasets.hex_checks import assert_h3_columns_unsigned


def _fetch(con):
    return lambda sql: con.execute(sql).fetchall()


def _write(con, path, select):
    con.execute(f"COPY ({select}) TO '{path}' (FORMAT PARQUET)")


class TestAssertH3ColumnsUnsigned:
    def test_passes_when_all_h_columns_ubigint(self):
        """UBIGINT native + parent columns satisfy the invariant."""
        with tempfile.TemporaryDirectory() as tmp:
            con = duckdb.connect()
            path = os.path.join(tmp, "data_0.parquet")
            _write(
                con,
                path,
                "SELECT 1.5 AS value, "
                "613196575302221823::UBIGINT AS h10, "
                "599718724986109951::UBIGINT AS h8, "
                "577199624117288959 AS h0",
            )
            # Should not raise.
            assert_h3_columns_unsigned(_fetch(con), path)
            con.close()

    def test_raises_on_signed_bigint_parent(self):
        """A signed BIGINT h-column (old h3_cell_to_parent) fails the build."""
        with tempfile.TemporaryDirectory() as tmp:
            con = duckdb.connect()
            path = os.path.join(tmp, "data_0.parquet")
            _write(
                con,
                path,
                "SELECT 1.5 AS value, "
                "613196575302221823::UBIGINT AS h10, "
                "599718724986109951::BIGINT AS h8, "  # offender
                "577199624117288959 AS h0",
            )
            with pytest.raises(RuntimeError, match=r"h8 \(BIGINT\)"):
                assert_h3_columns_unsigned(_fetch(con), path)
            con.close()

    def test_h0_exempt_when_read_as_bigint(self):
        """h0 is the hive key (read back as BIGINT) and must not trip the check."""
        with tempfile.TemporaryDirectory() as tmp:
            con = duckdb.connect()
            # Hive-partitioned layout so h0 is inferred from the path as BIGINT.
            part = os.path.join(tmp, "h0=577199624117288959")
            os.makedirs(part)
            _write(
                con,
                os.path.join(part, "data_0.parquet"),
                "SELECT 1.5 AS value, 613196575302221823::UBIGINT AS h10",
            )
            glob = os.path.join(tmp, "h0=*/data_0.parquet")
            assert con.execute(
                f"SELECT typeof(h0) FROM read_parquet('{glob}') LIMIT 1"
            ).fetchone()[0] == "BIGINT"
            # h0 BIGINT must be ignored; only h{N>=1} are checked.
            assert_h3_columns_unsigned(_fetch(con), glob)
            con.close()

    def test_value_columns_named_like_h_are_not_matched(self):
        """A non-index column such as 'h2o' or 'height' must not be flagged."""
        with tempfile.TemporaryDirectory() as tmp:
            con = duckdb.connect()
            path = os.path.join(tmp, "data_0.parquet")
            _write(
                con,
                path,
                "SELECT 1.5 AS height, 'x' AS h2o, "
                "613196575302221823::UBIGINT AS h8, "
                "577199624117288959 AS h0",
            )
            assert_h3_columns_unsigned(_fetch(con), path)
            con.close()
