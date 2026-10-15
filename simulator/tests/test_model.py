import unittest
from dataclasses import replace
from datetime import date

import numpy as np

import helpers
from fleet_sim.config import FleetConfig
from fleet_sim.model import METRICS, FleetModel

EPOCH = date(1970, 1, 1)


def day_no(d: date) -> int:
    return (d - EPOCH).days


MONDAY = day_no(date(2026, 1, 5))
SATURDAY = day_no(date(2026, 1, 3))


class DeterminismTests(unittest.TestCase):
    def test_same_seed_gives_identical_data(self):
        cfg = FleetConfig(services=2, instances_per_service=3)
        a = FleetModel(cfg).service_day(1, MONDAY)
        b = FleetModel(cfg).service_day(1, MONDAY)
        np.testing.assert_array_equal(a.ts_ms, b.ts_ms)
        for name, _ in METRICS:
            np.testing.assert_array_equal(a.metrics[name], b.metrics[name], err_msg=name)
        self.assertEqual(a.event_ids_for_mask(a.keep), b.event_ids_for_mask(b.keep))

    def test_different_seed_gives_different_data(self):
        a = FleetModel(FleetConfig(services=1, instances_per_service=3, seed=1)).service_day(0, MONDAY)
        b = FleetModel(FleetConfig(services=1, instances_per_service=3, seed=2)).service_day(0, MONDAY)
        self.assertFalse(np.array_equal(a.metrics["cpuUsagePercent"], b.metrics["cpuUsagePercent"]))

    def test_seed_drives_every_random_stream_not_just_the_profiles(self):
        """Profiles also depend on the seed, which could mask a stream that ignores it.
        Giving both models the SAME profiles isolates the day-noise, event-id and drop streams."""
        cfg1 = FleetConfig(services=1, instances_per_service=3, seed=1, drop_rate=0.1)
        cfg2 = replace(cfg1, seed=2)
        a, b = FleetModel(cfg1), FleetModel(cfg2)
        b.profiles = a.profiles
        da, db = a.service_day(0, MONDAY), b.service_day(0, MONDAY)
        self.assertFalse(np.array_equal(da.metrics["httpRequestsPerSec"], db.metrics["httpRequestsPerSec"]),
                         "day noise ignores the seed")
        self.assertFalse(np.array_equal(da.keep, db.keep), "drop mask ignores the seed")
        full = np.ones_like(da.keep)
        self.assertNotEqual(da.event_ids_for_mask(full)[:50], db.event_ids_for_mask(full)[:50],
                            "event ids ignore the seed")

    def test_generation_order_does_not_matter(self):
        cfg = FleetConfig(services=3, instances_per_service=2)
        forward = FleetModel(cfg)
        forward.service_day(0, MONDAY)
        forward.service_day(2, MONDAY)
        last_first = FleetModel(cfg)
        last_first.service_day(2, MONDAY)
        np.testing.assert_array_equal(
            forward.service_day(2, MONDAY).metrics["httpRequestsPerSec"],
            last_first.service_day(2, MONDAY).metrics["httpRequestsPerSec"],
        )

    def test_days_differ_but_are_reproducible(self):
        model = FleetModel(FleetConfig(services=1, instances_per_service=2))
        d1, d2 = model.service_day(0, MONDAY), model.service_day(0, MONDAY + 1)
        self.assertFalse(np.array_equal(d1.metrics["cpuUsagePercent"], d2.metrics["cpuUsagePercent"]))


class VolumeTests(unittest.TestCase):
    def test_full_fleet_produces_exactly_2_016_000_events_per_day(self):
        cfg = FleetConfig.from_yaml(helpers.DEFAULT_CONFIG)
        model = FleetModel(cfg)
        total = sum(int(model.service_day(s, MONDAY).keep.sum()) for s in range(cfg.services))
        self.assertEqual(total, 2_016_000)

    def test_drop_rate_removes_about_that_share(self):
        cfg = FleetConfig(services=1, instances_per_service=10, drop_rate=0.02)
        sd = FleetModel(cfg).service_day(0, MONDAY)
        kept = sd.keep.mean()
        self.assertAlmostEqual(kept, 0.98, delta=0.004)


class TimestampTests(unittest.TestCase):
    def setUp(self):
        self.cfg = FleetConfig(services=1, instances_per_service=4)
        self.sd = FleetModel(self.cfg).service_day(0, MONDAY)

    def test_each_instance_samples_every_interval_exactly(self):
        diffs = np.diff(self.sd.ts_ms, axis=1)
        self.assertTrue((diffs == self.cfg.interval_seconds * 1000).all())

    def test_day_is_fully_covered(self):
        start_ms = MONDAY * 86_400_000
        self.assertTrue((self.sd.ts_ms >= start_ms).all())
        self.assertTrue((self.sd.ts_ms < start_ms + 86_400_000).all())
        self.assertEqual(self.sd.steps, 5_760)

    def test_instances_are_not_aligned(self):
        self.assertGreater(len(set(self.sd.ts_ms[:, 0].tolist())), 1)


class EventIdTests(unittest.TestCase):
    def test_ids_are_unique_uuid_v7(self):
        sd = FleetModel(FleetConfig(services=1, instances_per_service=10)).service_day(0, MONDAY)
        ids = sd.event_ids_for_mask(sd.keep)
        self.assertEqual(len(ids), 57_600)
        self.assertEqual(len(set(ids)), 57_600)
        for sample in ids[:200]:
            self.assertRegex(sample, helpers.UUID_V7)

    def test_event_at_matches_bulk_ids(self):
        sd = FleetModel(FleetConfig(services=1, instances_per_service=3)).service_day(0, MONDAY)
        bulk = sd.event_ids_for_mask(sd.keep).__iter__()
        for inst in range(3):
            for step in range(sd.steps):
                expected = next(bulk)
                if step in (0, 1, 5_759):
                    self.assertEqual(sd.event_at(inst, step)["eventId"], expected)


class RealismTests(unittest.TestCase):
    """The data must look like a real fleet, not white noise (spec 9.1)."""

    @classmethod
    def setUpClass(cls):
        cfg = FleetConfig(services=4, instances_per_service=10)
        cls.model = FleetModel(cfg)
        cls.weekday = [cls.model.service_day(s, MONDAY) for s in range(4)]
        cls.weekend = [cls.model.service_day(s, SATURDAY) for s in range(4)]

    def test_values_stay_in_valid_ranges(self):
        for sd in self.weekday:
            m = sd.metrics
            self.assertTrue(((m["cpuUsagePercent"] >= 0) & (m["cpuUsagePercent"] <= 100)).all())
            self.assertTrue(((m["httpErrorRatePercent"] >= 0) & (m["httpErrorRatePercent"] <= 100)).all())
            self.assertTrue((m["memoryUsedBytes"] <= m["memoryLimitBytes"]).all())
            self.assertTrue((m["activeThreadCount"] >= 1).all())
            for name, _ in METRICS:
                self.assertTrue((m[name] >= 0).all(), name)
                self.assertTrue(np.isfinite(m[name]).all(), name)

    def test_traffic_has_a_daily_cycle(self):
        for sd in self.weekday:
            rps = sd.metrics["httpRequestsPerSec"]
            hour = (sd.ts_ms % 86_400_000) // 3_600_000
            by_hour = np.array([rps[hour == h].mean() for h in range(24)])
            self.assertGreater(by_hour.max() / by_hour.min(), 1.3, sd.profile.name)

    def test_weekend_traffic_is_lower(self):
        for wd, we in zip(self.weekday, self.weekend):
            ratio = we.metrics["httpRequestsPerSec"].mean() / wd.metrics["httpRequestsPerSec"].mean()
            self.assertLess(ratio, 0.95, wd.profile.name)

    def test_metrics_are_correlated_with_load(self):
        for sd in self.weekday:
            rps = sd.metrics["httpRequestsPerSec"].ravel()
            for name in ("cpuUsagePercent", "activeThreadCount", "networkRxBytesPerSec"):
                corr = np.corrcoef(rps, sd.metrics[name].ravel())[0, 1]
                self.assertGreater(corr, 0.8, f"{sd.profile.name}: rps vs {name}")

    def test_services_differ_from_each_other(self):
        means = {sd.profile.name: float(sd.metrics["httpRequestsPerSec"].mean()) for sd in self.weekday}
        self.assertGreater(len(set(round(v) for v in means.values())), 1)

    def test_normal_traffic_has_no_incident_like_cpu_saturation(self):
        for sd in self.weekday:
            self.assertLess(float(sd.metrics["cpuUsagePercent"].max()), 95.0, sd.profile.name)


if __name__ == "__main__":
    unittest.main()
