package com.telemetry.gateway.api;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.telemetry.gateway.auth.Agent;
import com.telemetry.gateway.auth.ApiKeyAuthenticator;
import com.telemetry.gateway.config.GatewayProperties;
import com.telemetry.gateway.publish.RawEventPublisher;
import com.telemetry.gateway.validation.BatchValidator;
import com.telemetry.gateway.validation.ValidationOutcome;
import com.telemetry.gateway.validation.ValidationOutcome.Checked;
import com.telemetry.gateway.validation.ValidationOutcome.EnvelopeInvalid;
import com.telemetry.gateway.validation.ValidationOutcome.Rejection;
import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.buffer.DataBufferLimitException;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

/**
 * POST /api/v1/telemetry/ingest (spec 8.1, FR-ING-01, FR-ING-03).
 *
 * <p>Checks run in this order, the first failure decides the answer:
 * <pre>
 *  X-API-Key missing or unknown ............ 401
 *  Content-Type not application/json ....... 415   (also Content-Encoding, until gzip arrives in week 5)
 *  body larger than 256 KiB ................ 413
 *  body is not JSON ........................ 400
 *  more than 100 events .................... 413
 *  envelope breaks the schema .............. 400
 *  agent.agentId is not the key's agent .... 403
 *  events checked one by one, valid ones published to Kafka, then 202 with accepted/rejected counts
 *  Kafka does not acknowledge in time ...... 503 with Retry-After
 * </pre>
 * Errors use RFC 9457 problem details (application/problem+json).
 */
@RestController
public class IngestController {

    private static final Logger log = LoggerFactory.getLogger(IngestController.class);
    static final String REQUEST_ID = "X-Request-Id";

    private final ApiKeyAuthenticator authenticator;
    private final BatchValidator validator;
    private final RawEventPublisher publisher;
    private final ObjectMapper mapper;
    private final GatewayProperties props;
    private final Clock clock;

    public IngestController(
            ApiKeyAuthenticator authenticator,
            BatchValidator validator,
            RawEventPublisher publisher,
            ObjectMapper mapper,
            GatewayProperties props,
            Clock clock) {
        this.authenticator = authenticator;
        this.validator = validator;
        this.publisher = publisher;
        this.mapper = mapper;
        this.props = props;
        this.clock = clock;
    }

    /** The 202 body of spec 8.1. */
    public record IngestResponse(String batchId, int accepted, int rejected, List<Rejection> rejections) {}

    @PostMapping("/api/v1/telemetry/ingest")
    public Mono<ResponseEntity<Object>> ingest(ServerHttpRequest request) {
        Instant receivedAt = clock.instant();
        HttpHeaders headers = request.getHeaders();
        String requestId = headers.getFirst(REQUEST_ID);

        Optional<Agent> agent = authenticator.authenticate(headers.getFirst("X-API-Key"));
        if (agent.isEmpty()) {
            return problem(HttpStatus.UNAUTHORIZED, "Missing or invalid API key", requestId);
        }
        MediaType contentType = headers.getContentType();
        if (contentType == null || !MediaType.APPLICATION_JSON.equalsTypeAndSubtype(contentType)) {
            return problem(HttpStatus.UNSUPPORTED_MEDIA_TYPE, "Content-Type must be application/json", requestId);
        }
        String encoding = headers.getFirst(HttpHeaders.CONTENT_ENCODING);
        if (encoding != null && !encoding.equalsIgnoreCase("identity")) {
            return problem(
                    HttpStatus.UNSUPPORTED_MEDIA_TYPE,
                    "Content-Encoding " + encoding + " is not supported yet",
                    requestId);
        }
        if (headers.getContentLength() > props.maxBodyBytes()) {
            return tooLarge(requestId);
        }

        // read at most maxBodyBytes; a longer body (e.g. chunked, no Content-Length) fails with
        // DataBufferLimitException
        return DataBufferUtils.join(request.getBody(), props.maxBodyBytes())
                .map(buffer -> {
                    byte[] bytes = new byte[buffer.readableByteCount()];
                    buffer.read(bytes);
                    DataBufferUtils.release(buffer);
                    return bytes;
                })
                .defaultIfEmpty(new byte[0])
                .flatMap(body -> handleBody(body, agent.get(), receivedAt, requestId))
                .onErrorResume(DataBufferLimitException.class, e -> tooLarge(requestId));
    }

    private Mono<ResponseEntity<Object>> handleBody(byte[] body, Agent agent, Instant receivedAt, String requestId) {
        JsonNode batch;
        try {
            batch = mapper.readTree(body); // NaN, Infinity and comments are refused by Jackson's defaults
        } catch (JsonProcessingException e) {
            return problem(HttpStatus.BAD_REQUEST, "Malformed JSON: " + e.getOriginalMessage(), requestId);
        } catch (IOException e) {
            return problem(HttpStatus.BAD_REQUEST, "Unreadable request body", requestId);
        }
        if (batch == null || batch.isMissingNode()) {
            return problem(HttpStatus.BAD_REQUEST, "Empty request body", requestId);
        }
        JsonNode events = batch.path("events");
        if (events.isArray() && events.size() > props.maxEvents()) {
            return problem(
                    HttpStatus.PAYLOAD_TOO_LARGE, "A batch holds at most " + props.maxEvents() + " events", requestId);
        }

        ValidationOutcome outcome = validator.validate(batch, agent.allowedServices());
        if (outcome instanceof EnvelopeInvalid invalid) {
            ProblemDetail pd = ProblemDetail.forStatusAndDetail(
                    HttpStatus.BAD_REQUEST, "The batch envelope does not match schema 1.0");
            pd.setProperty("errors", invalid.errors());
            return respond(pd, requestId);
        }
        String batchAgentId = batch.get("agent").get("agentId").asText();
        if (!batchAgentId.equals(agent.agentId())) {
            return problem(HttpStatus.FORBIDDEN, "agent.agentId does not match the API key", requestId);
        }

        Checked checked = (Checked) outcome;
        String batchId = batch.get("batchId").asText();
        IngestResponse response = new IngestResponse(
                batchId, checked.accepted().size(), checked.rejections().size(), checked.rejections());
        if (checked.accepted().isEmpty()) {
            return Mono.just(withRequestId(ResponseEntity.accepted(), requestId).body(response));
        }
        return publisher
                .publish(checked.accepted(), agent.agentId(), batchId, receivedAt)
                .then(Mono.fromSupplier(() ->
                        withRequestId(ResponseEntity.accepted(), requestId).body((Object) response)))
                .onErrorResume(e -> {
                    log.warn(
                            "batch {} of {}: Kafka did not acknowledge, answering 503: {}",
                            batchId,
                            agent.agentId(),
                            e.toString());
                    ProblemDetail pd = ProblemDetail.forStatusAndDetail(
                            HttpStatus.SERVICE_UNAVAILABLE, "The event store is unavailable, retry later");
                    return Mono.just(withRequestId(ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE), requestId)
                            .header(
                                    HttpHeaders.RETRY_AFTER,
                                    Long.toString(props.retryAfter().toSeconds()))
                            .contentType(MediaType.APPLICATION_PROBLEM_JSON)
                            .body(pd));
                });
    }

    private Mono<ResponseEntity<Object>> tooLarge(String requestId) {
        return problem(
                HttpStatus.PAYLOAD_TOO_LARGE, "Request body larger than " + props.maxBodyBytes() + " bytes", requestId);
    }

    private Mono<ResponseEntity<Object>> problem(HttpStatus status, String detail, String requestId) {
        return respond(ProblemDetail.forStatusAndDetail(status, detail), requestId);
    }

    private static Mono<ResponseEntity<Object>> respond(ProblemDetail pd, String requestId) {
        return Mono.just(withRequestId(ResponseEntity.status(pd.getStatus()), requestId)
                .contentType(MediaType.APPLICATION_PROBLEM_JSON)
                .body(pd));
    }

    /** X-Request-Id is echoed when the client sent one (spec 8.1). */
    private static ResponseEntity.BodyBuilder withRequestId(ResponseEntity.BodyBuilder builder, String requestId) {
        return requestId == null ? builder : builder.header(REQUEST_ID, requestId);
    }
}
