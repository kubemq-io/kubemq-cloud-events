package io.kubemq.examples.queues.ackall;

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
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.UUID;

/**
 * Example: queues/ack-all
 *
 * Sends 5 messages, peeks to verify count, then calls ack_all to drain
 * the queue atomically, then peeks again to confirm empty.
 *
 * Run: mvn compile exec:java
 */
public class Main {
    static String serverUrl() {
        String u = System.getenv("KUBEMQ_CE_URL");
        return (u != null && !u.isEmpty()) ? u : "http://localhost:9090";
    }
    static final ObjectMapper MAPPER = new ObjectMapper();

    static int peekCount(HttpClient client, String base, String channel, String clientId) throws Exception {
        String url = base + "/ce/queue/receive?channel=" + channel
                + "&client_id=" + clientId + "&max_messages=100&wait_timeout=3&is_peek=true";
        HttpResponse<String> resp = client.send(
                HttpRequest.newBuilder().uri(URI.create(url))
                        .POST(HttpRequest.BodyPublishers.noBody()).build(),
                HttpResponse.BodyHandlers.ofString());
        Map<?, ?> r = MAPPER.readValue(resp.body(), Map.class);
        Map<?, ?> data = (Map<?, ?>) r.get("data");
        if (data == null) return 0;
        Object val = data.get("messages_received");
        return val != null ? ((Number) val).intValue() : 0;
    }

    public static void main(String[] args) throws Exception {
        String base = serverUrl();
        String channel = "java-ce-queues.ack-all";
        String clientId = "kubemq-ce-java-example";

        EventFormatProvider.getInstance().registerFormat(new JsonFormat());
        EventFormat format = EventFormatProvider.getInstance().resolveFormat(JsonFormat.CONTENT_TYPE);
        HttpClient httpClient = HttpClient.newHttpClient();

        // Send 5 messages.
        System.out.println("Sending 5 messages to queue...");
        for (int i = 1; i <= 5; i++) {
            CloudEvent ev = CloudEventBuilder.v1()
                    .withId(UUID.randomUUID().toString())
                    .withType("com.kubemq.examples.queues.ackall")
                    .withSource(URI.create("kubemq-ce-java-example"))
                    .withSubject(channel)
                    .withDataContentType("application/json")
                    .withTime(OffsetDateTime.now())
                    .withData("application/json", MAPPER.writeValueAsBytes(Map.of("n", i)))
                    .build();
            httpClient.send(
                    HttpRequest.newBuilder().uri(URI.create(base + "/ce/queue/send"))
                            .POST(HttpRequest.BodyPublishers.ofByteArray(format.serialize(ev)))
                            .header("Content-Type", "application/cloudevents+json").build(),
                    HttpResponse.BodyHandlers.ofString());
        }

        System.out.println("Peek before ack_all: " + peekCount(httpClient, base, channel, clientId) + " messages");

        // ack_all — drain queue atomically.
        String ackUrl = base + "/ce/queue/ack_all?channel=" + channel
                + "&client_id=" + clientId + "&wait_timeout=5";
        HttpResponse<String> ackResp = httpClient.send(
                HttpRequest.newBuilder().uri(URI.create(ackUrl))
                        .POST(HttpRequest.BodyPublishers.noBody()).build(),
                HttpResponse.BodyHandlers.ofString());
        Map<?, ?> ackResult = MAPPER.readValue(ackResp.body(), Map.class);
        System.out.println("ack_all: is_error=" + ackResult.get("is_error") + " message=" + ackResult.get("message"));

        System.out.println("Peek after ack_all: " + peekCount(httpClient, base, channel, clientId) + " messages remaining");
    }
}

// Expected output:
// Sending 5 messages to queue...
// Peek before ack_all: 5 messages
// ack_all: is_error=false message=OK
// Peek after ack_all: 0 messages remaining
