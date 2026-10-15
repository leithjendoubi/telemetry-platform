import contextlib
import io
import json
import tempfile
import unittest
from datetime import date
from pathlib import Path

import helpers
from fleet_sim import cli
from fleet_sim.config import FleetConfig
from fleet_sim.offline import COLUMNS, OutputError, generate_dataset

try:
    import pyarrow  # noqa: F401
    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False

SMALL = FleetConfig(services=2, instances_per_service=3)  # 6 instances -> 34,560 rows per day
START = date(2026, 1, 1)


class CsvDatasetTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.root = Path(self._tmp.name)

    def gen(self, name, cfg=SMALL, days=2, fmt="csv"):
        return generate_dataset(cfg, self.root / name, start=START, days=days, fmt=fmt)

    def test_row_counts_and_files(self):
        m = self.gen("a")
        self.assertEqual(m["totals"]["rows"], 2 * 3 * 5_760 * 2)
        self.assertEqual(m["totals"]["rows"], m["totals"]["expectedRowsWithoutDrops"])
        self.assertEqual([f["path"] for f in m["files"]],
                         ["date=2026-01-01/part-0000.csv.gz", "date=2026-01-02/part-0000.csv.gz"])
        for f in m["files"]:
            self.assertTrue((self.root / "a" / f["path"]).exists())
        self.assertTrue((self.root / "a" / "incidents.jsonl").exists())
        self.assertEqual(m["totals"]["labelPositive"], 0)

    def test_manifest_on_disk_matches_returned_manifest(self):
        m = self.gen("a")
        on_disk = json.loads((self.root / "a" / "manifest.json").read_text())
        self.assertEqual(on_disk, m)
        self.assertEqual(on_disk["columns"], list(COLUMNS))
        self.assertEqual(on_disk["configSha256"], SMALL.sha256())
        self.assertEqual(on_disk["dataset"]["start"], "2026-01-01")

    def test_file_content_is_sorted_and_complete(self):
        import pandas as pd

        m = self.gen("a", days=1)
        df = pd.read_csv(self.root / "a" / m["files"][0]["path"])
        self.assertEqual(list(df.columns), list(COLUMNS))
        times = pd.to_datetime(df["event_time"], utc=True)
        self.assertTrue(times.is_monotonic_increasing)
        self.assertEqual(df["event_id"].nunique(), len(df))
        self.assertEqual(df["instance_id"].nunique(), 6)
        self.assertTrue((df["label"] == 0).all())
        self.assertTrue(times.dt.strftime("%Y-%m-%d").eq("2026-01-01").all())

    def test_same_seed_reproduces_the_dataset_bit_for_bit(self):
        a, b = self.gen("a"), self.gen("b")
        self.assertEqual(a["datasetContentSha256"], b["datasetContentSha256"])
        self.assertEqual([f["contentSha256"] for f in a["files"]], [f["contentSha256"] for f in b["files"]])
        # the gzip header carries no timestamp, so even the file bytes are identical
        self.assertEqual([f["sha256"] for f in a["files"]], [f["sha256"] for f in b["files"]])

    def test_different_seed_changes_the_dataset(self):
        a = self.gen("a")
        b = generate_dataset(FleetConfig(services=2, instances_per_service=3, seed=7),
                             self.root / "b", start=START, days=2, fmt="csv")
        self.assertNotEqual(a["datasetContentSha256"], b["datasetContentSha256"])

    def test_dropped_samples_reduce_row_count_and_are_reported(self):
        cfg = FleetConfig(services=2, instances_per_service=3, drop_rate=0.05)
        m = self.gen("d", cfg=cfg, days=1)
        self.assertLess(m["totals"]["rows"], m["totals"]["expectedRowsWithoutDrops"])
        self.assertGreater(m["totals"]["rows"], 0.93 * m["totals"]["expectedRowsWithoutDrops"])

    def test_invalid_arguments(self):
        with self.assertRaises(OutputError):
            generate_dataset(SMALL, self.root / "x", start=START, days=0)
        with self.assertRaises(OutputError):
            generate_dataset(SMALL, self.root / "x", start=START, days=1, fmt="xml")


@unittest.skipIf(HAS_PYARROW, "pyarrow is installed; the friendly-error path cannot be exercised")
class MissingPyarrowTests(unittest.TestCase):
    def test_parquet_without_pyarrow_explains_what_to_do(self):
        with tempfile.TemporaryDirectory() as tmp, self.assertRaises(OutputError) as ctx:
            generate_dataset(SMALL, Path(tmp) / "p", start=START, days=1, fmt="parquet")
        self.assertIn("pip install pyarrow", str(ctx.exception))
        self.assertIn("--format csv", str(ctx.exception))


@unittest.skipUnless(HAS_PYARROW, "needs pyarrow (pip install pyarrow)")
class ParquetDatasetTests(unittest.TestCase):
    def test_parquet_and_csv_contain_the_same_data(self):
        import pyarrow.parquet as pq

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            c = generate_dataset(SMALL, root / "c", start=START, days=1, fmt="csv")
            p = generate_dataset(SMALL, root / "p", start=START, days=1, fmt="parquet")
            self.assertEqual(c["datasetContentSha256"], p["datasetContentSha256"])
            table = pq.read_table(root / "p" / p["files"][0]["path"])
            self.assertEqual(table.column_names, list(COLUMNS))
            self.assertEqual(table.num_rows, 34_560)
            self.assertEqual(str(table.schema.field("event_time").type), "timestamp[ms, tz=UTC]")


class CliTests(unittest.TestCase):
    def run_cli(self, *argv):
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = cli.main(list(argv))
        return code, out.getvalue(), err.getvalue()

    def test_describe_prints_the_spec_volumes(self):
        code, out, _ = self.run_cli("describe", "--config", str(helpers.DEFAULT_CONFIG))
        self.assertEqual(code, 0)
        self.assertIn("2,016,000", out)
        self.assertIn("350", out)

    def test_generate_small_dataset(self):
        with tempfile.TemporaryDirectory() as tmp:
            code, out, _ = self.run_cli(
                "generate", "--config", str(helpers.DEFAULT_CONFIG), "--out", tmp + "/ds",
                "--services", "1", "--instances", "2", "--days", "1", "--format", "csv",
            )
            self.assertEqual(code, 0, out)
            self.assertIn("11,520 rows", out)

    def test_bad_input_gives_exit_code_2_and_a_message(self):
        code, _, err = self.run_cli("describe", "--config", str(helpers.DEFAULT_CONFIG), "--services", "99")
        self.assertEqual(code, 2)
        self.assertIn("error:", err)


if __name__ == "__main__":
    unittest.main()
