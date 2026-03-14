import polars as pl
import pytest
from datetime import datetime, timezone
from pathlib import Path


def make_hourly_df(times: list[datetime]) -> pl.DataFrame:
    return pl.DataFrame({"time": times, "value": [1.0] * len(times)})


class TestFindMaxLocalTime:
    def test_returns_none_when_no_files(self, tmp_path):
        from archive import find_max_local_time
        result = find_max_local_time(tmp_path, "branch_energy_hourly")
        assert result is None

    def test_returns_max_across_single_file(self, tmp_path):
        from archive import find_max_local_time
        t1 = datetime(2026, 1, 1, 10, tzinfo=timezone.utc)
        t2 = datetime(2026, 1, 1, 12, tzinfo=timezone.utc)
        df = make_hourly_df([t1, t2])
        df.write_parquet(tmp_path / "branch_energy_hourly_20260101_20260101.parquet")
        result = find_max_local_time(tmp_path, "branch_energy_hourly")
        assert result == t2

    def test_returns_max_across_multiple_files(self, tmp_path):
        from archive import find_max_local_time
        t1 = datetime(2026, 1, 1, tzinfo=timezone.utc)
        t2 = datetime(2026, 2, 1, tzinfo=timezone.utc)
        t3 = datetime(2026, 3, 1, tzinfo=timezone.utc)
        make_hourly_df([t1, t2]).write_parquet(
            tmp_path / "branch_energy_hourly_20260101_20260201.parquet"
        )
        make_hourly_df([t3]).write_parquet(
            tmp_path / "branch_energy_hourly_20260301_20260301.parquet"
        )
        result = find_max_local_time(tmp_path, "branch_energy_hourly")
        assert result == t3

    def test_ignores_files_for_other_tables(self, tmp_path):
        from archive import find_max_local_time
        t = datetime(2026, 3, 1, tzinfo=timezone.utc)
        make_hourly_df([t]).write_parquet(
            tmp_path / "main_energy_hourly_20260301_20260301.parquet"
        )
        result = find_max_local_time(tmp_path, "branch_energy_hourly")
        assert result is None

    def test_does_not_match_table_name_prefix(self, tmp_path):
        from archive import find_max_local_time
        t = datetime(2026, 3, 1, tzinfo=timezone.utc)
        # branch_energy_hourly files must not be matched when looking for branch_energy
        make_hourly_df([t]).write_parquet(
            tmp_path / "branch_energy_hourly_20260301_20260301.parquet"
        )
        result = find_max_local_time(tmp_path, "branch_energy")
        assert result is None


class TestMakeFilename:
    def test_formats_date_range_from_dataframe(self):
        from archive import make_filename
        t1 = datetime(2024, 8, 26, tzinfo=timezone.utc)
        t2 = datetime(2026, 3, 14, tzinfo=timezone.utc)
        df = make_hourly_df([t1, t2])
        result = make_filename("branch_energy_hourly", df)
        assert result == "branch_energy_hourly_20240826_20260314.parquet"

    def test_single_day_range(self):
        from archive import make_filename
        t = datetime(2026, 3, 14, tzinfo=timezone.utc)
        df = make_hourly_df([t])
        result = make_filename("main_energy_hourly", df)
        assert result == "main_energy_hourly_20260314_20260314.parquet"
