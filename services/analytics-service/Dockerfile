# syntax=docker/dockerfile:1
# Analytics service image (spec 14.2): multi-stage build, Spring Boot layered jar, JRE 21, non-root.
# Build context = repository root, because the build copies libs/telemetry-contracts into the jar:
#   docker build -f services/analytics-service/Dockerfile .
# Tests are not run here: they need Docker themselves (Testcontainers). Run `mvn test` before building.

FROM maven:3.9.16-eclipse-temurin-21 AS build
WORKDIR /src
COPY libs/telemetry-contracts libs/telemetry-contracts
COPY services/analytics-service/pom.xml services/analytics-service/pom.xml
RUN --mount=type=cache,target=/root/.m2 mvn -B -q -f services/analytics-service/pom.xml dependency:go-offline
COPY services/analytics-service/src services/analytics-service/src
RUN --mount=type=cache,target=/root/.m2 mvn -B -q -f services/analytics-service/pom.xml package -Dmaven.test.skip=true \
 && java -Djarmode=tools -jar services/analytics-service/target/analytics-service-0.1.0.jar \
         extract --layers --launcher --destination /layers

FROM eclipse-temurin:21.0.12.1_1-jre
RUN useradd --system --uid 10001 --no-create-home --shell /usr/sbin/nologin app
WORKDIR /app
COPY --from=build /layers/dependencies/ ./
COPY --from=build /layers/spring-boot-loader/ ./
COPY --from=build /layers/snapshot-dependencies/ ./
COPY --from=build /layers/application/ ./
USER 10001
EXPOSE 8082
HEALTHCHECK --interval=10s --timeout=3s --start-period=60s --retries=5 \
  CMD curl -fsS http://localhost:8082/health/ready || exit 1
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=60", "org.springframework.boot.loader.launch.JarLauncher"]
