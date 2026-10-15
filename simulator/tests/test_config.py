import unittest

import helpers  # noqa: F401  (sets up sys.path)
from fleet_sim.config import SERVICE_NAMES, ConfigError, FleetConfig


class ConfigTests(unittest.TestCase):
    def test_service_list_has_35_valid_unique_identifiers(self):
        import re

        self.assertEqual(len(SERVICE_NAMES), 35)
        self.assertEqual(len(set(SERVICE_NAMES)), 35)
        pattern = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
        for name in SERVICE_NAMES:
            self.assertRegex(name, pattern)

    def test_default_yaml_matches_spec_fleet_model(self):
        cfg = FleetConfig.from_yaml(helpers.DEFAULT_CONFIG)
        self.assertEqual(cfg.services, 35)
        self.assertEqual(cfg.instances, 350)
        self.assertEqual(cfg.steps_per_day, 5_760)
        self.assertEqual(cfg.events_per_day, 2_016_000)          # spec 4.1
        self.assertAlmostEqual(cfg.events_per_second, 23.33, places=2)
        self.assertEqual(cfg.drop_rate, 0.0)

    def test_agent_and_api_key_naming(self):
        cfg = FleetConfig()
        self.assertEqual(cfg.agent_id(0), "agent-checkout-api")
        self.assertEqual(cfg.api_key(0), "dev-agent-checkout-api")

    def test_scenarios_must_stay_empty_in_v0(self):
        with self.assertRaises(ConfigError):
            FleetConfig.from_dict({"scenarios": [{"id": "S1"}]})

    def test_unknown_keys_are_rejected(self):
        with self.assertRaises(ConfigError):
            FleetConfig.from_dict({"fleet": {"colour": "blue"}})
        with self.assertRaises(ConfigError):
            FleetConfig.from_dict({"surprise": 1})

    def test_invalid_values_are_rejected(self):
        for kwargs in ({"services": 0}, {"services": 36}, {"interval_seconds": 7},
                       {"drop_rate": 1.0}, {"environment": "production"}, {"instances_per_service": 0}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ConfigError):
                FleetConfig(**kwargs)

    def test_config_hash_changes_with_config(self):
        self.assertNotEqual(FleetConfig(seed=1).sha256(), FleetConfig(seed=2).sha256())
        self.assertEqual(FleetConfig(seed=1).sha256(), FleetConfig(seed=1).sha256())


if __name__ == "__main__":
    unittest.main()
