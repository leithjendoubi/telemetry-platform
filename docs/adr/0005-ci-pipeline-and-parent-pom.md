# ADR-0005: CI pipeline and parent POM

- Status: accepted
- Date: 2026-11-05
- Context: gate G1 asks for the smoke test in CI (spec 16.2 week 4); spec 14.1 asks for a parent POM that pins
  versions; spec 14.4 lists the pipeline stages; spec 13.5 sets the coverage minimums.

## Decisions

1. **One GitHub Actions workflow** (`.github/workflows/ci.yml`) with the six stages of spec 14.4 as jobs, chained
   with `needs`: static checks -> unit tests + security (in parallel) -> image build and scan -> integration tests ->
   smoke test. A later stage only runs when the earlier ones are green, so a red run points at the first broken stage.
   It runs on every push and by hand (`workflow_dispatch`). ESLint, npm audit, Angular tests and AsyncAPI validation
   join when those parts exist.
2. **Root `pom.xml` is the parent of both Java services and their aggregator.** It pins library and plugin versions
   and carries the shared quality plugins: Spotless (palantir-java-format) and Checkstyle (`config/checkstyle/`),
   Failsafe, JaCoCo, OWASP Dependency-Check (`config/dependency-check/`). Each service still builds alone with
   `mvn -f services/<name>/pom.xml` (found through `relativePath`); the Dockerfiles therefore copy the root `pom.xml`.
   `repo.root` points at the repository root both from a service folder and, through the `at-repo-root` profile,
   from the root itself.
3. **Unit and integration tests are split by name.** `*Test` = fast, no Docker (Surefire, `mvn test`, stage 2);
   `*IT` = Testcontainers against real Kafka/TimescaleDB (Failsafe, `mvn verify`, stage 5). Python uses the
   `integration` pytest marker the same way.
4. **Coverage gates** (spec 13.5): Java >= 80 % lines and >= 70 % branches, unit and integration tests counted
   together, bootstrap/config classes excluded; Python detection service >= 80 %. The detection gate needs the
   Kafka tests, so it runs in stage 5 where CI starts the compose Kafka.
5. **Architecture rules are tests.** ArchUnit in each Java service, import-linter in the detection service, so a
   forbidden dependency (e.g. `auth` importing `config` in the gateway) fails stage 2.
6. **Security tools fail on High/Critical only**: Semgrep at severity ERROR, Dependency-Check at CVSS >= 7, Trivy
   HIGH/CRITICAL with `--ignore-unfixed`, pip-audit, Gitleaks over the whole history. Dependency-Check needs the
   GitHub secret `NVD_API_KEY` (without it the vulnerability database download takes hours) and is skipped when
   the secret is missing; the README states whether it actually ran.
7. **Tool versions are pinned** (actions by major version, tool images and Python tools by exact version), so a
   tool update cannot turn the pipeline red without a commit.

## Consequences

- Formatting is enforced: run `mvn spotless:apply` and `ruff format .` before committing.
- `mvn test` no longer runs the Testcontainers tests; use `mvn verify` for the full Java suite.
- `.gitattributes` forces LF line endings so the formatters see the same bytes on Windows and on the Linux runner.
