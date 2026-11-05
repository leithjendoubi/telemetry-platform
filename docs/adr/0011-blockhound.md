# ADR-0011: BlockHound in the gateway tests

- Status: accepted. Designed by Claude within FR-ING-02 and spec 13.5; no choice was put to Leith in this step.
- Date: 2026-11-05
- Context: week 5, step A6. FR-ING-02: requests are handled on Netty event-loop threads without blocking I/O,
  "enforced by BlockHound in the test suite"; spec 13.5: zero detections.

## Decisions

1. **BlockHound runs in every gateway test** (`blockhound-junit-platform` 1.0.18.RELEASE, test scope; it installs
   itself through the JUnit platform). On Java 21 it needs the JVM flag `-XX:+AllowRedefinitionToAddDeleteMethods`,
   set for Surefire and Failsafe next to JaCoCo's `@{argLine}` (an empty default keeps it working with
   `-Djacoco.skip`). Without the flag the build fails loudly; `BlockHoundIsActiveTest` fails if BlockHound is not
   installed at all, so a green run cannot mean "BlockHound was off".
2. **What BlockHound found, and the fixes** (all one-time work that happened on the first request):
   - the date library behind `format: date-time` reads its leap-second table from its jar, and the validator reads
     its message file on the first error. Fix: `BatchValidator` validates one valid and one broken sample batch at
     start-up (`warmUp`); the message file stays in memory afterwards (networknt keeps it in its own maps). The
     validator's language is fixed to English, so there is only one message file and the messages are the same on
     every machine.
   - reactor-kafka created the Kafka producer on the first send, i.e. on a Netty thread (class loading with file
     reads, then `close()` with `Object.wait` after the failure). Fix: the producer is created when the `KafkaSender`
     bean is built. That needs no broker, so the gateway still starts while Kafka is down.
3. **A dedicated test for the producer** (`KafkaSenderStartupTest`): BlockHound alone could not guard it, because a
   test's own Kafka client loading the classes first hides the late creation (a planted bug went unnoticed in one
   order). The test checks the producer's JMX registration right after the bean is built, with a unique client id,
   independent of test order. A fresh JVM per test class (`reuseForks=false`) was tried and dropped: Failsafe then
   failed in the Maven process itself.

4. **One allowance, found by CI on Linux** (`BlockHoundAllowances`, registered in `META-INF/services`): when R2DBC opens
   a new database connection, PostgreSQL's SCRAM-SHA-256 login asks `SecureRandom` for a nonce, and on Linux the
   default `NativePRNG` reads it from `/dev/urandom` on the Netty thread. Windows uses an OS call, so it never showed
   locally. The read never waits and happens only when the pool opens a connection; replacing the JVM-wide random
   source would change security behaviour for the whole application. So blocking is allowed inside exactly one
   method, `com.ongres.scram.common.util.CryptoUtil.nonce`. `BlockHoundIsActiveTest` proves the allowance is loaded and
   covers only that method (the same slow random source used elsewhere is still reported).

## Consequences

Any new blocking call on an event-loop thread fails the test that triggers it. Libraries that do lazy I/O on first
use must be warmed up at start-up, with a comment saying why.
