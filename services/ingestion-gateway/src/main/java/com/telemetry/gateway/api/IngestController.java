package com.telemetry.gateway.api;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.telemetry.gateway.auth.Agent;
import com.telemetry.gateway.auth.ApiKeyAuthenticator;
import com.telemetry.gateway.auth.ApiKeyAuthenticator.AuthResult;
import com.telemetry.gateway.config.GatewayProperties;
import com.telemetry.gateway.publish.EventPublisher;
import com.telemetry.gateway.ratelimit.AgentRateLimiter;
import com.telemetry.gateway.ratelimit.AgentRateLimiter.Decision;
import com.telemetry.gateway.ratelimit.InFlightLimiter;
import com.telemetry.gateway.registry.AgentRegistry;
import com.telemetry.gateway.validation.BatchValidator;
import com.telemetry.gateway.validation.ValidationOutcome;
import com.telemetry.gateway.validation.ValidationOutcome.Checked;
import com.telemetry.gateway.validation.ValidationOutcome.EnvelopeInvalid;
import com.telemetry.gateway.validation.ValidationOutcome.Rejection;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
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
 *  2,000 requests already in progress ...... 503 with Retry-After (FR-ING-06, spec 6.8)
 *  X-API-Key missing or unknown ............ 401   (agents: ingestion_db via AgentRegistry, 10 s cache)
 *  X-API-Key of a revoked agent ............ 403
 *  agent registry does not answer .......... 503 with Retry-After
 *  agent over its own rate limit ........... 429 with Retry-After (token bucket from table agent, FR-ING-06)
 *  Content-Type not application/json ....... 415   (also Content-Encoding, until gzip arrives in week 5)
 *  body larger than 256 KiB ................ 413
 *  body is not JSON ........................ 400
 *  more than 100 events .................... 413
 *  envelope breaks the schema .............. 400
 *  agent.agentId is not the key's agent .... 403
 *  events checked one by one; valid ones published to telemetry.raw.v1, invalid ones to telemetry.rejected.v1,
 *  then 202 with accepted/rejected counts
 *  Kafka does not acknowledge in time ...... 503 with Retry-After
 * </pre>
 * Errors use RFC 9457 problem details (application/problem+json).
 */
@RestController
public class IngestController {

    private static final Logger log = LoggerFactory.getLogger(IngestController.class);
    static final String REQUEST_ID = "X-Request-Id";
    /** Longest wait for the rest of a body before an early answer (see {@link #afterBody}). */
    static final Duration DRAIN_TIMEOUT = Duration.ofSeconds(5);

    private final AgentRegistry registry;
    private final AgentRateLimiter rateLimiter;
    private final InFlightLimiter inFlight;
    private final BatchValidator validator;
    private final EventPublisher publisher;
    private final ObjectMapper mapper;
    private final GatewayProperties props;
    private final Clock clock;

    public IngestController(
            AgentRegistry registry,
            AgentRateLimiter rateLimiter,
            InFlightLimiter inFlight,
            BatchValidator validator,
            EventPublisher publisher,
            ObjectMapper mapper,
            GatewayProperties props,
            Clock clock) {
        this.registry = registry;
        this.rateLimiter = rateLimiter;
        this.inFlight = inFlight;
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
        return Mono.defer(() -> {
            String requestId = request.getHeaders().getFirst(REQUEST_ID);
            if (!inFlight.tryAcquire()) {
                // overload: answer at once, without reading the body (that would add load); a client that loses
                // this answer sees a network error, which it retries like the 503 (spec 8.1)
                return unavailable("Too many requests in progress, retry later", requestId);
            }
            // the slot is freed however the request ends: answer, error, or the client hanging up
            return checkAgent(request, requestId).doFinally(signal -> inFlight.release());
        });
    }

    private Mono<ResponseEntity<Object>> checkAgent(ServerHttpRequest request, String requestId) {
        Instant receivedAt = clock.instant();
        Optional<String> keyHash =
                ApiKeyAuthenticator.keyHash(request.getHeaders().getFirst("X-API-Key"));
        if (keyHash.isEmpty()) {
            return afterBody(request, problem(HttpStatus.UNAUTHORIZED, "Missing or invalid API key", requestId));
        }
        return registry.findByKeyHash(keyHash.get())
                .onErrorMap(RegistryUnavailable::new) // only the lookup's own failures mean "registry down"
                .map(ApiKeyAuthenticator::decide)
                .flatMap(result -> switch (result) {
                    case AuthResult.Unknown unknown ->
                        afterBody(request, problem(HttpStatus.UNAUTHORIZED, "Missing or invalid API key", requestId));
                    case AuthResult.Revoked revoked ->
                        afterBody(request, problem(HttpStatus.FORBIDDEN, "The API key has been revoked", requestId));
                    case AuthResult.Authenticated ok ->
                        switch (rateLimiter.tryAcquire(ok.agent())) {
                            case Decision.Allowed allowed -> authenticated(request, ok.agent(), receivedAt, requestId);
                            case Decision.Limited limited ->
                                afterBody(request, tooManyRequests(limited.retryAfterSeconds(), requestId));
                        };
                })
                .onErrorResume(RegistryUnavailable.class, e -> {
                    log.warn(
                            "agent registry unavailable, answering 503: {}",
                            e.getCause().toString());
                    return afterBody(request, unavailable("The agent registry is unavailable, retry later", requestId));
                });
    }

    /**
     * Sends an answer that was decided before the body was needed (401, 403, 415, 503) only after the body has
     * arrived, reading and dropping it. Otherwise the server closes the connection while the client is still sending,
     * and the client loses the answer ("connection aborted", seen with the simulator on 2026-11-05: a revoked agent
     * saw network errors and retried instead of stopping). Bounded like a normal read: at most maxBodyBytes and
     * {@link #DRAIN_TIMEOUT}; after that the answer goes out anyway.
     */
    private Mono<ResponseEntity<Object>> afterBody(ServerHttpRequest request, Mono<ResponseEntity<Object>> answer) {
        return DataBufferUtils.takeUntilByteCount(request.getBody(), props.maxBodyBytes())
                .doOnNext(DataBufferUtils::release)
                .then()
                .timeout(DRAIN_TIMEOUT)
                .onErrorResume(e -> Mono.empty())
                .then(answer);
    }

    /** The checks after authentication; a registry failure must not be confused with an error in here. */
    private Mono<ResponseEntity<Object>> authenticated(
            ServerHttpRequest request, Agent agent, Instant receivedAt, String requestId) {
        HttpHeaders headers = request.getHeaders();
        MediaType contentType = headers.getContentType();
        if (contentType == null || !MediaType.APPLICATION_JSON.equalsTypeAndSubtype(contentType)) {
            return afterBody(
                    request,
                    problem(HttpStatus.UNSUPPORTED_MEDIA_TYPE, "Content-Type must be application/json", requestId));
        }
        String encoding = headers.getFirst(HttpHeaders.CONTENT_ENCODING);
        if (encoding != null && !encoding.equalsIgnoreCase("identity")) {
            return afterBody(
                    request,
                    problem(
                            HttpStatus.UNSUPPORTED_MEDIA_TYPE,
                            "Content-Encoding " + encoding + " is not supported yet",
                            requestId));
        }
        if (headers.getContentLength() > props.maxBodyBytes()) {
            return tooLarge(requestId); // not drained: the body is announced as too large
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
                .flatMap(body -> handleBody(body, agent, receivedAt, requestId))
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
        // accepted and rejected events are published together; even a batch with nothing accepted
        // has its rejections recorded on telemetry.rejected.v1 before the 202 (AT-02)
        return publisher
                .publish(
                        checked.accepted(),
                        checked.rejections(),
                        batch.get("events"),
                        agent.agentId(),
                        batchId,
                        receivedAt)
                .then(Mono.fromSupplier(() ->
                        withRequestId(ResponseEntity.accepted(), requestId).body((Object) response)))
                .onErrorResume(e -> {
                    log.warn(
                            "batch {} of {}: Kafka did not acknowledge, answering 503: {}",
                            batchId,
                            agent.agentId(),
                            e.toString());
                    return unavailable("The event store is unavailable, retry later", requestId);
                });
    }

    /** 429 with Retry-After (spec 8.1, FR-ING-06): this agent is over its own rate limit. */
    private Mono<ResponseEntity<Object>> tooManyRequests(long retryAfterSeconds, String requestId) {
        ProblemDetail pd = ProblemDetail.forStatusAndDetail(
                HttpStatus.TOO_MANY_REQUESTS, "Rate limit of this agent exceeded, retry later");
        return Mono.just(withRequestId(ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS), requestId)
                .header(HttpHeaders.RETRY_AFTER, Long.toString(retryAfterSeconds))
                .contentType(MediaType.APPLICATION_PROBLEM_JSON)
                .body(pd));
    }

    /** 503 with Retry-After (spec 8.1): the agent keeps the batch and sends it again later. */
    private Mono<ResponseEntity<Object>> unavailable(String detail, String requestId) {
        ProblemDetail pd = ProblemDetail.forStatusAndDetail(HttpStatus.SERVICE_UNAVAILABLE, detail);
        return Mono.just(withRequestId(ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE), requestId)
                .header(
                        HttpHeaders.RETRY_AFTER,
                        Long.toString(props.retryAfter().toSeconds()))
                .contentType(MediaType.APPLICATION_PROBLEM_JSON)
                .body(pd));
    }

    /** The agent registry (ingestion_db) could not answer; never treated as "unknown key". */
    private static final class RegistryUnavailable extends RuntimeException {
        RegistryUnavailable(Throwable cause) {
            super(cause);
        }
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
