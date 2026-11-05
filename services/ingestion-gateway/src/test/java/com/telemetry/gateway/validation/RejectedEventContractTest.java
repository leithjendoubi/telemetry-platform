package com.telemetry.gateway.validation;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.networknt.schema.JsonSchema;
import com.telemetry.gateway.ContractFiles;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/** The gateway's side of telemetry-rejected-event.1.0.json (topic telemetry.rejected.v1, FR-ING-07, AT-02). */
class RejectedEventContractTest {

    private static final String SCHEMA = "telemetry-rejected-event.1.0.json";

    @Test
    void rejectionCodesAreExactlyThoseOfTheContract() {
        JsonNode schemaJson = ContractFiles.read("contracts/schemas/" + SCHEMA);
        Set<String> contractCodes = new HashSet<>();
        schemaJson.at("/properties/code/enum").forEach(code -> contractCodes.add(code.asText()));

        Set<String> gatewayCodes =
                Arrays.stream(RejectionCode.values()).map(Enum::name).collect(Collectors.toSet());

        assertThat(gatewayCodes).isEqualTo(contractCodes);
    }

    @Test
    void javaValidatorAcceptsTheExampleAndRejectsABrokenCopy() {
        JsonSchema schema = ContractFiles.schema(SCHEMA);
        ObjectNode example = (ObjectNode) ContractFiles.read("contracts/examples/rejected-event.json");

        assertThat(schema.validate(example)).isEmpty();
        assertThat(schema.validate(example.deepCopy().put("code", "UNKNOWN_CODE")))
                .isNotEmpty();
        assertThat(schema.validate(example.deepCopy().put("batchId", "not-a-uuid")))
                .isNotEmpty();
    }
}
