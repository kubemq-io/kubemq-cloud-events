package io.kubemq.examples.events.contentmodes;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.core.format.EventFormat;
import io.cloudevents.core.provider.EventFormatProvider;
import io.cloudevents.jackson.JsonFormat;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.UUID;

public class Main {
    static String serverUrl() {
        String u = System.getenv("KUBEMQ_CE_URL");
        return (u != null && !u.isEmpty()) ? u : "http://localhost:9090";
    }
    static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        String base = serverUrl();
        EventFormatProvider.getInstance().registerFormat(new JsonFormat());
        EventFormat format = EventFormatProvider.getInstance().resolveFormat(JsonFormat.CONTENT_TYPE);
        HttpClient httpClient = HttpClient.newHttpClient();

        CloudEvent event = CloudEventBuilder.v1()
                .withId(UUID.randomUUID().toString())
                .withType("com.kubemq.examples.events.content-mode")
                .withSource(URI.create("kubemq-ce-java-example"))
                .withSubject("java-ce-events.content-modes")
                .withDataContentType("application/json")
                .withTime(OffsetDateTime.now())
                .withData("application/json",
                        MAPPER.writeValueAsBytes(Map.of("message", "hello content modes")))
                .build();

        // --- Structured mode ---
        System.out.println("Sending in structured mode:");
        byte[] structuredBody = format.serialize(event);
        HttpResponse<String> r1 = httpClient.send(
                HttpRequest.newBuilder().uri(URI.create(base + "/ce/send/event"))
                        .POST(HttpRequest.BodyPublishers.ofByteArray(structuredBody))
                        .header("Content-Type", "application/cloudevents+json").build(),
                HttpResponse.BodyHandlers.ofString());
        Map<?, ?> res1 = MAPPER.readValue(r1.body(), Map.class);
        System.out.printf("[structured] status=%d is_error=%s%n", r1.statusCode(), res1.get("is_error"));

        // --- Binary mode ---
        System.out.println("\nSending in binary mode:");
        byte[] data = event.getData() != null ? event.getData().toBytes() : new byte[0];
        HttpResponse<String> r2 = httpClient.send(
                HttpRequest.newBuilder().uri(URI.create(base + "/ce/send/event"))
                        .POST(HttpRequest.BodyPublishers.ofByteArray(data))
                        .header("Content-Type", "application/json")
                        .header("ce-specversion", "1.0")
                        .header("ce-type", event.getType())
                        .header("ce-source", event.getSource().toString())
                        .header("ce-id", event.getId())
                        .header("ce-subject", event.getSubject())
                        .header("ce-time", event.getTime().toString())
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        Map<?, ?> res2 = MAPPER.readValue(r2.body(), Map.class);
        System.out.printf("[binary]     status=%d is_error=%s%n", r2.statusCode(), res2.get("is_error"));

        System.out.println("\nBoth content modes accepted.");
    }
}

// Expected output:
// Sending in structured mode:
// [structured] status=202 is_error=false
//
// Sending in binary mode:
// [binary]     status=202 is_error=false
//
// Both content modes accepted.
