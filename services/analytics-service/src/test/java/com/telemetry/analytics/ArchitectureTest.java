package com.telemetry.analytics;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static com.tngtech.archunit.library.dependencies.SlicesRuleDefinition.slices;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;
import org.springframework.web.bind.annotation.RestController;

/**
 * Architecture rules of the analytics service (spec 7.4, 13.5: ArchUnit, zero violations).
 * The write side (ingest: Kafka to TimescaleDB) and the read side (query: HTTP API) are independent, so
 * either can change or be scaled without touching the other.
 */
@AnalyzeClasses(packages = "com.telemetry.analytics", importOptions = ImportOption.DoNotIncludeTests.class)
class ArchitectureTest {

    @ArchTest
    static final ArchRule NO_PACKAGE_CYCLES =
            slices().matching("com.telemetry.analytics.(*)..").should().beFreeOfCycles();

    @ArchTest
    static final ArchRule WRITE_AND_READ_SIDES_ARE_INDEPENDENT = noClasses()
            .that()
            .resideInAPackage("..analytics.ingest..")
            .should()
            .dependOnClassesThat()
            .resideInAPackage("..analytics.query..")
            .orShould()
            .dependOnClassesThat()
            .resideInAnyPackage("org.springframework.web..", "org.springframework.http..");

    @ArchTest
    static final ArchRule READ_SIDE_DOES_NOT_KNOW_KAFKA = noClasses()
            .that()
            .resideInAPackage("..analytics.query..")
            .should()
            .dependOnClassesThat()
            .resideInAnyPackage("..analytics.ingest..", "org.apache.kafka..", "org.springframework.kafka..");

    @ArchTest
    static final ArchRule PARSER_IS_FRAMEWORK_FREE = noClasses()
            .that()
            .haveSimpleNameStartingWith("ScoredEventParser")
            .or()
            .haveSimpleName("TelemetryRow")
            .should()
            .dependOnClassesThat()
            .resideInAnyPackage("org.springframework..", "org.apache.kafka..")
            .because("turning a record into a row is pure logic, unit-testable without Kafka or a database");

    @ArchTest
    static final ArchRule CONTROLLERS_LIVE_IN_QUERY =
            classes().that().areAnnotatedWith(RestController.class).should().resideInAPackage("..analytics.query..");
}
