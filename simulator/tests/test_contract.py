"""Contract tests: the simulator's payloads must satisfy libs/telemetry-contracts (spec 8.1).

The same files are reused by the Java gateway tests in stage 2, so both sides test one contract.
"""

import json
import re
import unittest
from datetime import UTC, datetime, timedelta

import helpers

from fleet_sim.config import FleetConfig
from fleet_sim.live import LiveRunner
from fleet_sim.model import FleetModel


class ContractFilesTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.validator = helpers.make_validator()
        cls.valid = helpers.load_json(helpers.VALID_EXAMPLE)
        cls.cases = helpers.load_json(helpers.INVALID_CASES)["cases"]

    def test_schema_is_itself_valid(self):
        from jsonschema import Draft202012Validator

        Draft202012Validator.check_schema(helpers.load_json(helpers.SCHEMA_PATH))

    def test_patterns_use_the_ecma_end_of_text(self):
        self.assertEqual(helpers.ecma_anchors("^[a-z]{0,3}$"), r"^[a-z]{0,3}\Z")
        self.assertEqual(helpers.ecma_anchors(r"^a\$b$"), r"^a\$b\Z")  # an escaped $ is a literal dollar
        self.assertEqual(helpers.ecma_anchors("^[$]+$"), r"^[$]+\Z")  # inside [...] too
        doc = helpers.load_json(helpers.VALID_EXAMPLE)
        doc["events"][0]["resource"]["instanceId"] = "checkout-api-07\n"
        self.assertTrue(
            list(self.validator.iter_errors(doc)), "a trailing newline is not the end of the text"
        )

    def test_valid_example_passes(self):
        errors = list(self.validator.iter_errors(self.valid))
        self.assertEqual([e.message for e in errors], [])

    def test_batch_size_limit_is_100_events(self):
        import copy

        def batch_of(n):
            doc = copy.deepcopy(self.valid)
            doc["events"] = [copy.deepcopy(self.valid["events"][0]) for _ in range(n)]
            return doc

        self.assertEqual(list(self.validator.iter_errors(batch_of(100))), [])
        self.assertTrue(list(self.validator.iter_errors(batch_of(101))))

    def test_every_invalid_case_is_rejected_by_the_expected_layer(self):
        now = datetime.now(UTC)
        base = helpers.load_json(helpers.VALID_EXAMPLE)
        # make the base fresh so only the case's own change can trigger the 24 h / 5 min rule
        base["agent"]["sentAt"] = helpers.iso_ms_dt(now)
        base["events"][0]["timestamp"] = helpers.iso_ms_dt(now - timedelta(seconds=30))
        self.assertEqual(list(self.validator.iter_errors(base)), [])
        self.assertEqual(helpers.semantic_errors(base["events"][0], now), [])

        for case in self.cases:
            with self.subTest(case=case["name"]):
                doc = helpers.apply_case(base, case, now)
                schema_errors = list(self.validator.iter_errors(doc))
                if case["expect"] == "SCHEMA":
                    self.assertTrue(schema_errors, "schema should reject this document")
                else:
                    self.assertEqual([e.message for e in schema_errors], [], "schema should accept it")
                    self.assertIn(case["code"], helpers.semantic_errors(doc["events"][0], now))


class SimulatorPayloadTests(unittest.TestCase):
    def test_generated_batches_validate_against_the_schema_and_rules(self):
        validator = helpers.make_validator()
        cfg = FleetConfig(services=5, instances_per_service=10)
        runner = LiveRunner(FleetModel(cfg), url="http://unused.invalid", speed=1.0)
        now = datetime.now(UTC)
        tick = int(now.timestamp()) // 15 * 15
        seen_ids = set()
        for s in range(cfg.services):
            batch = runner.build_batch(s, tick)
            self.assertEqual(list(validator.iter_errors(batch)), [], batch["agent"]["agentId"])
            self.assertLessEqual(len(batch["events"]), 100)
            self.assertEqual(batch["agent"]["agentId"], cfg.agent_id(s))
            for event in batch["events"]:
                self.assertRegex(event["timestamp"], helpers.ISO_MS)
                self.assertEqual(helpers.semantic_errors(event, now + timedelta(seconds=15)), [])
                self.assertEqual(
                    event["resource"]["serviceName"], batch["events"][0]["resource"]["serviceName"]
                )
                seen_ids.add(event["eventId"])
        self.assertEqual(len(seen_ids), 50)  # 5 services x 10 instances, all distinct

    def test_batch_timestamps_fall_inside_their_interval(self):
        cfg = FleetConfig(services=1, instances_per_service=10)
        runner = LiveRunner(FleetModel(cfg), url="http://unused.invalid", speed=1.0)
        tick = 1_800_000_000 // 15 * 15
        batch = runner.build_batch(0, tick)
        for event in batch["events"]:
            ts = helpers.parse_ts(event["timestamp"]).timestamp()
            self.assertGreaterEqual(ts, tick)
            self.assertLess(ts, tick + 15)
        sent_at = helpers.parse_ts(batch["agent"]["sentAt"]).timestamp()
        self.assertEqual(sent_at, tick + 15)


class RawEventContractTests(unittest.TestCase):
    """The Kafka record of telemetry.raw.v1 = one batch event + receivedAt, agentId, batchId (spec 8.3)."""

    def test_raw_event_schema_is_valid_and_example_passes(self):
        from jsonschema import Draft202012Validator, FormatChecker

        schema = helpers.load_json(helpers.RAW_EVENT_SCHEMA_PATH)
        Draft202012Validator.check_schema(schema)
        validator = Draft202012Validator(schema, format_checker=FormatChecker())
        example = helpers.load_json(helpers.RAW_EVENT_EXAMPLE)
        self.assertEqual([e.message for e in validator.iter_errors(example)], [])
        for extra in ("receivedAt", "agentId", "batchId"):
            broken = dict(example)
            del broken[extra]
            self.assertTrue(list(validator.iter_errors(broken)), f"{extra} must be required")

    def test_event_fields_are_identical_to_the_batch_schema(self):
        batch = helpers.load_json(helpers.SCHEMA_PATH)
        raw = helpers.load_json(helpers.RAW_EVENT_SCHEMA_PATH)
        event = batch["$defs"]["event"]
        for name, definition in event["properties"].items():
            self.assertEqual(raw["properties"][name], definition, name)
        self.assertEqual(set(raw["required"]), set(event["required"]) | {"receivedAt", "agentId", "batchId"})
        for name in ("identifier", "nonNegative", "percent"):
            self.assertEqual(raw["$defs"][name], batch["$defs"][name], name)


class ScoredEventContractTests(unittest.TestCase):
    """Kafka record of telemetry.scored.v1 = one raw record + prediction + scoredAt (spec 8.3, FR-DET-03)."""

    def test_scored_event_schema_is_valid_and_example_passes(self):
        from jsonschema import Draft202012Validator, FormatChecker

        schema = helpers.load_json(helpers.SCORED_EVENT_SCHEMA_PATH)
        Draft202012Validator.check_schema(schema)
        validator = Draft202012Validator(schema, format_checker=FormatChecker())
        example = helpers.load_json(helpers.SCORED_EVENT_EXAMPLE)
        self.assertEqual([e.message for e in validator.iter_errors(example)], [])
        broken = json.loads(json.dumps(example))
        broken["prediction"]["probability"] = 1.5
        self.assertTrue(list(validator.iter_errors(broken)), "probability must stay in [0, 1]")
        for field in ("prediction", "scoredAt"):
            broken = dict(example)
            del broken[field]
            self.assertTrue(list(validator.iter_errors(broken)), f"{field} must be required")

    def test_raw_fields_are_identical_to_the_raw_schema(self):
        raw = helpers.load_json(helpers.RAW_EVENT_SCHEMA_PATH)
        scored = helpers.load_json(helpers.SCORED_EVENT_SCHEMA_PATH)
        for name, definition in raw["properties"].items():
            self.assertEqual(scored["properties"][name], definition, name)
        self.assertEqual(set(scored["required"]), set(raw["required"]) | {"prediction", "scoredAt"})
        self.assertEqual(scored["$defs"], raw["$defs"])


class RejectedEventContractTests(unittest.TestCase):
    """Record of telemetry.rejected.v1: one 202 rejections entry + agentId, batchId, receivedAt, event."""

    @classmethod
    def setUpClass(cls):
        from jsonschema import Draft202012Validator, FormatChecker

        cls.schema = helpers.load_json(helpers.REJECTED_EVENT_SCHEMA_PATH)
        Draft202012Validator.check_schema(cls.schema)
        cls.validator = Draft202012Validator(cls.schema, format_checker=FormatChecker())
        cls.example = helpers.load_json(helpers.REJECTED_EVENT_EXAMPLE)

    def errors(self, doc):
        return [e.message for e in self.validator.iter_errors(doc)]

    def test_example_passes_and_every_field_is_required(self):
        self.assertEqual(self.errors(self.example), [])
        # spec 8.1 rejections entry + the raw record's additions + the event fragment of spec 8.3
        expected = {"index", "eventId", "code", "message", "agentId", "batchId", "receivedAt", "event"}
        self.assertEqual(set(self.schema["required"]), expected)
        for field in expected:
            broken = dict(self.example)
            del broken[field]
            self.assertTrue(self.errors(broken), f"{field} must be required")

    def test_untrusted_parts_may_be_anything_the_agent_sent(self):
        for event, event_id in (("not an object", None), ([1, 2], "not-a-uuid"), (None, None)):
            doc = dict(self.example, event=event, eventId=event_id, code="SCHEMA_VIOLATION")
            self.assertEqual(self.errors(doc), [], repr(event))

    def test_trusted_parts_are_checked(self):
        for field, value in (
            ("index", 100),
            ("code", "UNKNOWN_CODE"),
            ("agentId", "bad agent"),
            ("batchId", "x"),
        ):
            self.assertTrue(self.errors(dict(self.example, **{field: value})), f"{field}={value!r} must fail")

    def test_codes_are_those_of_the_contract_readme(self):
        readme = helpers.CONTRACTS_README.read_text(encoding="utf-8")
        section = readme.split("## What JSON Schema cannot check", 1)[1].split("\n## ", 1)[0]
        documented = set(re.findall(r"\|\s*`([A-Z_]+)`\s*\|", section))
        self.assertEqual(set(self.schema["properties"]["code"]["enum"]), documented)

    def test_shared_fields_are_defined_as_in_the_raw_schema(self):
        raw = helpers.load_json(helpers.RAW_EVENT_SCHEMA_PATH)

        def without_description(d):
            return {k: v for k, v in d.items() if k != "description"}

        for name in ("agentId", "batchId", "receivedAt"):
            self.assertEqual(
                without_description(self.schema["properties"][name]),
                without_description(raw["properties"][name]),
                name,
            )
        self.assertEqual(self.schema["$defs"]["identifier"], raw["$defs"]["identifier"])


class AnalyticsOpenApiContractTests(unittest.TestCase):
    """The query API contract (spec 8.2) must be valid OpenAPI 3.1 (spec 14.4 stage 1)."""

    @classmethod
    def setUpClass(cls):
        from openapi_spec_validator.readers import read_from_filename

        cls.spec, _ = read_from_filename(str(helpers.ANALYTICS_OPENAPI_PATH))

    def test_document_is_valid_openapi_3_1(self):
        from openapi_spec_validator import OpenAPIV31SpecValidator

        errors = list(OpenAPIV31SpecValidator(self.spec).iter_errors())
        self.assertEqual([e.message for e in errors], [])

    def test_validator_rejects_a_broken_document(self):
        from openapi_spec_validator import OpenAPIV31SpecValidator

        broken = json.loads(json.dumps(self.spec))
        del broken["info"]  # required by OpenAPI 3.1
        self.assertTrue(list(OpenAPIV31SpecValidator(broken).iter_errors()))
        broken = json.loads(json.dumps(self.spec))
        broken["components"]["schemas"]["Series"]["required"] = "points"  # must be an array
        self.assertTrue(list(OpenAPIV31SpecValidator(broken).iter_errors()))

    def test_metric_names_are_those_of_the_ingest_contract(self):
        batch = helpers.load_json(helpers.SCHEMA_PATH)
        ingest_metrics = set(batch["$defs"]["event"]["properties"]["metrics"]["properties"])
        params = self.spec["paths"]["/api/v1/analytics/instances/{instanceId}/series"]["get"]["parameters"]
        metric = next(p for p in params if p["name"] == "metric")
        self.assertEqual(set(metric["schema"]["enum"]), ingest_metrics)


if __name__ == "__main__":
    unittest.main()
