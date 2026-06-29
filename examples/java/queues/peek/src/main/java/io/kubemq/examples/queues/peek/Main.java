package io.kubemq.examples.queues.peek;

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
 * Example: queues/peek
 *
 * Sends 3 messages to a queue, then peeks twice (is_peek=true) to inspect
 * without consuming, then receives (is_peek=false) to confirm messages remain.
 *
 * Run: mvn compile exec:java
 */
public class Main {
    static String serverUrl() {
        String u = System.getenv("KUBEMQ_CE_URL");
        return (u != null && !u.isEmpty()) ? u : "http://localhost:9090";
    }
    static final ObjectMapper MAPPER = new ObjectMapper();

    static int peekOrReceive(HttpClient client, String base, String channel,
                              String clientId, boolean isPeak) throws Exception {
        String url = base + "/ce/queue/receive?channel=" + channel
                + "&client_id=" + clientId
                + "&max_messages=10&wait_timeout=3&is_peek=" + isPeak;
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
        String channel = "java-ce-queues.peek";
        String clientId = "kubemq-ce-java-worker";

        EventFormatProvider.getInstance().registerFormat(new JsonFormat());
        EventFormat format = EventFormatProvider.getInstance().resolveFormat(JsonFormat.CONTENT_TYPE);
        HttpClient httpClient = HttpClient.newHttpClient();

        // Send 3 messages.
        System.out.println("Sending 3 messages to queue...");
        for (int i = 1; i <= 3; i++) {
            CloudEvent event = CloudEventBuilder.v1()
                    .withId(UUID.randomUUID().toString())
                    .withType("com.kubemq.examples.queues.peek")
                    .withSource(URI.create("kubemq-ce-java-example"))
                    .withSubject(channel)
                    .withDataContentType("application/json")
                    .withTime(OffsetDateTime.now())
                    .withData("application/json", MAPPER.writeValueAsBytes(Map.of("n", i)))
                    .build();
            HttpResponse<String> resp = httpClient.send(
                    HttpRequest.newBuilder().uri(URI.create(base + "/ce/queue/send"))
                            .POST(HttpRequest.BodyPublishers.ofByteArray(format.serialize(event)))
                            .header("Content-Type", "application/cloudevents+json").build(),
                    HttpResponse.BodyHandlers.ofString());
            System.out.println("  Sent message " + i + ": status=" + resp.statusCode());
        }

        // Peek twice — messages should remain.
        int peek1 = peekOrReceive(httpClient, base, channel, clientId, true);
        System.out.println("[peek #1] messages_received=" + peek1);
        int peek2 = peekOrReceive(httpClient, base, channel, clientId, true);
        System.out.println("[peek #2] messages_received=" + peek2);

        // Consume — messages are removed.
        int consumed = peekOrReceive(httpClient, base, channel, clientId, false);
        System.out.println("[consume] messages_received=" + consumed);
    }
}

// Expected output:
// Sending 3 messages to queue...
//   Sent message 1: status=202
//   Sent message 2: status=202
//   Sent message 3: status=202
// [peek #1] messages_received=3
// [peek #2] messages_received=3
// [consume] messages_received=3
