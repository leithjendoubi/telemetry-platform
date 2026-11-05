package com.telemetry.gateway;

import static com.tngtech.archunit.core.domain.properties.HasName.Predicates.nameMatching;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static com.tngtech.archunit.library.dependencies.SlicesRuleDefinition.slices;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

/**
 * Architecture rules of the gateway (spec 7.4, 13.5: ArchUnit, zero violations).
 * <pre>
 *   api  ──▶ auth, validation, publish        (the HTTP adapter uses everything)
 *   publish ──▶ validation                    (the Kafka adapter sends accepted events)
 *   auth, validation                          (pure rules: no Spring, no Kafka, no HTTP)
 *   config ──▶ everything                     (wiring only)
 * </pre>
 */
@AnalyzeClasses(packages = "com.telemetry.gateway", importOptions = ImportOption.DoNotIncludeTests.class)
class ArchitectureTest {

    @ArchTest
    static final ArchRule NO_PACKAGE_CYCLES =
            slices().matching("com.telemetry.gateway.(*)..").should().beFreeOfCycles();

    @ArchTest
    static final ArchRule RULES_ARE_FRAMEWORK_FREE = noClasses()
            .that()
            .resideInAnyPackage("..gateway.auth..", "..gateway.validation..")
            .should()
            .dependOnClassesThat()
            .resideInAnyPackage("org.springframework..", "reactor..", "org.apache.kafka..")
            .because("validation and authentication rules must be testable without the web or Kafka stack");

    @ArchTest
    static final ArchRule RULES_DO_NOT_KNOW_ADAPTERS = noClasses()
            .that()
            .resideInAnyPackage("..gateway.auth..", "..gateway.validation..")
            .should()
            .dependOnClassesThat()
            .resideInAnyPackage("..gateway.api..", "..gateway.publish..", "..gateway.config..");

    @ArchTest
    static final ArchRule PUBLISHER_DOES_NOT_KNOW_HTTP = noClasses()
            .that()
            .resideInAPackage("..gateway.publish..")
            .should()
            .dependOnClassesThat()
            .resideInAnyPackage("..gateway.api..", "org.springframework.web..", "org.springframework.http..");

    /**
     * Keeps the exception in .trivyignore.yaml true: CVE-2026-47884, -47890 and -47892 of Spring Framework 6.2
     * (fixed only in 7.x) need XSLT views, view fragments or WebFlux functional endpoints. Same rule in
     * analytics-service. See docs/adr/0006-accepted-spring-6-cves.md before changing it.
     */
    @ArchTest
    static final ArchRule NO_SPRING_FEATURES_OF_ACCEPTED_CVES = noClasses()
            .should()
            .dependOnClassesThat(nameMatching("org\\.springframework\\.web\\.reactive\\.function\\.server\\..*"
                    + "|org\\.springframework\\.web\\.servlet\\.view\\.xslt\\..*"
                    + "|org\\.springframework\\.web\\..*\\.Fragment(sRendering)?(\\$.*)?"))
            .because("the CVEs accepted in .trivyignore.yaml are only harmless while these features are unused");
}
