package com.telemetry.gateway;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

/**
 * One real database for all integration tests: the compose image, prepared by the real init script
 * (infra/compose/init-db/01-create-databases.sh), so the gateway connects exactly as in compose: database
 * ingestion_db, its own role "ingestion". The gateway's Flyway migrations then create the agent table and the
 * 35 dev agents. Started once per test run (singleton container), stopped by Testcontainers at the end.
 */
public final class TestDatabase {

    static final String INGESTION_USER = "ingestion";
    static final String INGESTION_PASSWORD = "ingestion_test";

    static final PostgreSQLContainer<?> DB = new PostgreSQLContainer<>(
                    DockerImageName.parse("timescale/timescaledb:2.17.2-pg16").asCompatibleSubstituteFor("postgres"))
            .withDatabaseName("postgres")
            .withUsername("postgres")
            .withPassword("test_super")
            .withEnv(Map.of(
                    "INGESTION_DB_USER", INGESTION_USER,
                    "INGESTION_DB_PASSWORD", INGESTION_PASSWORD,
                    "ANALYTICS_DB_USER", "analytics",
                    "ANALYTICS_DB_PASSWORD", "analytics_test",
                    "ALERTING_DB_USER", "alerting",
                    "ALERTING_DB_PASSWORD", "alerting_test"))
            .withCopyFileToContainer(
                    MountableFile.forClasspathResource("init-db/01-create-databases.sh", 0755),
                    "/docker-entrypoint-initdb.d/01-create-databases.sh");

    static {
        DB.start();
    }

    private TestDatabase() {}

    /** Points the gateway's R2DBC connection and Flyway at ingestion_db of the test database. */
    public static void register(DynamicPropertyRegistry registry) {
        registry.add("spring.r2dbc.url", () -> "r2dbc:postgresql://" + hostAndPort() + "/ingestion_db");
        registry.add("spring.r2dbc.username", () -> INGESTION_USER);
        registry.add("spring.r2dbc.password", () -> INGESTION_PASSWORD);
        registry.add("spring.flyway.url", TestDatabase::jdbcUrl);
        registry.add("spring.flyway.user", () -> INGESTION_USER);
        registry.add("spring.flyway.password", () -> INGESTION_PASSWORD);
    }

    /** Runs one statement in ingestion_db as the gateway's role, e.g. to add or revoke an agent. */
    public static int update(String sql, Object... params) {
        try (Connection c = DriverManager.getConnection(jdbcUrl(), INGESTION_USER, INGESTION_PASSWORD);
                PreparedStatement s = c.prepareStatement(sql)) {
            for (int i = 0; i < params.length; i++) {
                s.setObject(i + 1, params[i]);
            }
            return s.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    private static String jdbcUrl() {
        return "jdbc:postgresql://" + hostAndPort() + "/ingestion_db";
    }

    private static String hostAndPort() {
        return DB.getHost() + ":" + DB.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT);
    }
}
