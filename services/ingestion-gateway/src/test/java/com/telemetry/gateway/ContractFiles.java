package com.telemetry.gateway;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Test access to libs/telemetry-contracts/examples (copied onto the test classpath by pom.xml).
 * The simulator's Python tests read the very same files, so gateway and simulator cannot drift apart.
 */
public final class ContractFiles {

    public static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Pattern RELATIVE = Pattern.compile("^RELATIVE:([+-])(\\d+)([hm])$");

    /** One entry of invalid-cases.json. */
    public record InvalidCase(String name, JsonNode set, JsonNode remove, String expect, String code) {
        @Override
        public String toString() {
            return name; // shown as the test name
        }

        /** True when every change is inside /events/i/..., so a SCHEMA case rejects one event, not the whole batch. */
        public boolean touchesOnlyEvents() {
            List<String> pointers = new ArrayList<>();
            if (set != null) {
                set.fieldNames().forEachRemaining(pointers::add);
            }
            if (remove != null) {
                remove.forEach(p -> pointers.add(p.asText()));
            }
            return pointers.stream().allMatch(p -> p.startsWith("/events/"));
        }
    }

    private ContractFiles() {}

    public static JsonNode read(String resource) {
        try (InputStream in = ContractFiles.class.getClassLoader().getResourceAsStream(resource)) {
            if (in == null) {
                throw new IllegalStateException(resource + " not on the test classpath");
            }
            return MAPPER.readTree(in);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    /** valid-batch.json with fresh times (sentAt = now, event timestamp = now - 30 s), so only a case's own change can fail it. */
    public static ObjectNode freshValidBatch(Instant now) {
        ObjectNode batch = (ObjectNode) read("contracts/examples/valid-batch.json");
        ((ObjectNode) batch.get("agent")).put("sentAt", iso(now));
        ((ObjectNode) batch.get("events").get(0)).put("timestamp", iso(now.minusSeconds(30)));
        return batch;
    }

    public static List<InvalidCase> invalidCases() {
        List<InvalidCase> cases = new ArrayList<>();
        for (JsonNode c : read("contracts/examples/invalid-cases.json").get("cases")) {
            cases.add(new InvalidCase(
                    c.get("name").asText(),
                    c.get("set"),
                    c.get("remove"),
                    c.get("expect").asText(),
                    c.get("code").asText()));
        }
        return cases;
    }

    /** A copy of {@code base} with the case's JSON-Pointer changes applied. */
    public static ObjectNode apply(ObjectNode base, InvalidCase invalidCase, Instant now) {
        ObjectNode doc = base.deepCopy();
        if (invalidCase.set() != null) {
            for (Map.Entry<String, JsonNode> change : invalidCase.set().properties()) {
                setAt(doc, change.getKey(), resolveRelative(change.getValue(), now));
            }
        }
        if (invalidCase.remove() != null) {
            for (JsonNode pointer : invalidCase.remove()) {
                JsonPointer ptr = JsonPointer.compile(pointer.asText());
                ((ObjectNode) doc.at(ptr.head())).remove(ptr.last().getMatchingProperty());
            }
        }
        return doc;
    }

    public static String iso(Instant instant) {
        return DateTimeFormatter.ISO_INSTANT.format(instant.truncatedTo(ChronoUnit.MILLIS));
    }

    private static void setAt(ObjectNode doc, String pointer, JsonNode value) {
        JsonPointer ptr = JsonPointer.compile(pointer);
        JsonNode parent = doc.at(ptr.head());
        if (parent instanceof ArrayNode array) {
            array.set(ptr.last().getMatchingIndex(), value);
        } else {
            ((ObjectNode) parent).set(ptr.last().getMatchingProperty(), value);
        }
    }

    /** "RELATIVE:-25h" means now minus 25 hours (see the notes in invalid-cases.json). */
    private static JsonNode resolveRelative(JsonNode value, Instant now) {
        if (!value.isTextual()) {
            return value;
        }
        Matcher m = RELATIVE.matcher(value.asText());
        if (!m.matches()) {
            return value;
        }
        long amount = Long.parseLong(m.group(2));
        Duration shift = m.group(3).equals("h") ? Duration.ofHours(amount) : Duration.ofMinutes(amount);
        return TextNode.valueOf(iso(m.group(1).equals("+") ? now.plus(shift) : now.minus(shift)));
    }
}
