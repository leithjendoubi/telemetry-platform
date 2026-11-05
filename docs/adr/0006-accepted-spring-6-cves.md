# ADR-0006: Image-scan findings: patch what can be patched, guard what cannot

- Status: accepted. Decisions 3 and 4 were chosen by Leith on 2026-11-05 from options Claude offered (his answer:
  "1A, 2A"; 1A = guarded, time-boxed exception for the 3 Spring CVEs, 2A = upgrade apt in the image). Decisions 1 and 2
  (Spring Boot 3.5.16, patch pins) are patch upgrades within spec 6.5 that Claude made without asking.
- Date: 2026-11-05
- Context: the first CI run failed stage 4 (Trivy, HIGH/CRITICAL with a fix available) on all three images.
  Spec 6.5 fixes Spring Boot 3; spec 14.4 asks for an image scan in CI.

## Findings and decisions

1. **Outdated Spring Boot patch level (the cause of most findings).** Spring Boot 3.5.7 -> **3.5.16**, the newest 3.5.x.
   This alone fixed Spring Framework, spring-kafka, kafka-clients, Micrometer and most Tomcat/Netty/Jackson findings.
   Still within spec 6.5.
2. **Libraries still behind after 3.5.16.** Raised to the smallest fixing patch version in the parent POM
   (Jackson 2.21.7, Netty 4.1.137.Final, Tomcat 10.1.59, PostgreSQL driver 42.7.12, and kafka-clients' codecs lz4-java
   1.11.4, snappy-java 1.1.10.9). Same minor lines; `mvn verify` green (all integration tests). Each line is removed
   once Boot manages that version or newer.
3. **Base images.** Python `3.12.15-slim-trixie` -> `3.12.16-slim-trixie` (contains the fixed `apt`). The newest
   `eclipse-temurin:21.0.12.1_1-jre` still had `apt` CVE-2026-109518 (HIGH, fixed by Ubuntu): both Java Dockerfiles
   upgrade only `apt` and `libapt-pkg7.0` in the runtime stage, until a newer Temurin tag contains the fix.
4. **Three CRITICAL Spring Framework 6.2 CVEs fixed only in Spring Framework 7 (= Spring Boot 4):**
   CVE-2026-47884 (XsltView RCE), CVE-2026-47890 (SSE with view fragments), CVE-2026-47892 (WebFlux functional
   endpoint header predicates). Our code uses none of these features (only annotated `@RestController`s).
   Moving to Spring Boot 4 would change the spec 6.5 stack and is a migration of several days.
   **Decision: accept them for now, guarded and time-boxed.**
   - `.trivyignore.yaml` lists exactly these 3 ids, each limited to the exact package version (`@6.2.19`), with a
     reason and `expired_at: 2026-12-31`. A version change or the date makes Trivy report them again (CI red).
   - ArchUnit rule `NO_SPRING_FEATURES_OF_ACCEPTED_CVES` in both Java services fails the build if code starts using
     `web.reactive.function.server`, `web.servlet.view.xslt` or view `Fragment`/`FragmentsRendering`. Checked: planted
     uses of RouterFunction, FragmentsRendering and XsltView make it fail; RestController, WebClient and plain SSE
     are not affected.
   - Without the ignore file, the scan shows only these 3 CVEs (checked 2026-11-05), so nothing else is hidden.

## Consequences

- CI stage 4 can be green while Spring Framework 6.2 contains known CRITICAL bugs in unused features. README's
  "Honest status" says so.
- **To do before 2026-12-31:** discuss Spring Boot 4 / Spring Framework 7 with the supervisor (spec 6.5 change),
  or renew this decision with a new date and a fresh check that no other Spring CVE appeared.
