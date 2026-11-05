package com.telemetry.gateway.validation;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;

/** Result of {@link BatchValidator#validate}: either the whole batch is unusable, or each event got a verdict. */
public sealed interface ValidationOutcome {

    /** The envelope (everything outside the events) breaks the schema. The HTTP answer is 400. */
    record EnvelopeInvalid(List<String> errors) implements ValidationOutcome {}

    /** The envelope is fine. Some events are accepted, the others rejected one by one (partial acceptance, FR-ING-03). */
    record Checked(List<AcceptedEvent> accepted, List<Rejection> rejections) implements ValidationOutcome {}

    /** An event that passed every check; {@code index} is its position in the request's events array. */
    record AcceptedEvent(int index, ObjectNode event) {}

    /** One rejected event, as reported in the 202 response body of spec 8.1. */
    record Rejection(int index, String eventId, RejectionCode code, String message) {}
}
