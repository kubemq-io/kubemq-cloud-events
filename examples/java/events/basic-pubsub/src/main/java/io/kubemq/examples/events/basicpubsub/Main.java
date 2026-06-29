/**
 * Example: events/basic-pubsub
 *
 * Demonstrates basic fire-and-forget event pub/sub via the KubeMQ
 * CloudEvents HTTP connector.
 *
 * Run: mvn compile exec:java
 */
package io.kubemq.examples.events.basicpubsub;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.core.format.EventFormat;
import io.cloudevents.core.provider.EventFormatProvider;
import io.cloudevents.jackson.JsonFormat;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class Main {

    static String serverUrl() {
        String u = System.getenv("KUBEMQ_CE_URL");
        return (u != null && !u.isEmpty()) ? u : "http://localhost:9090";
    }

    static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        String base = serverUrl();
        String channel = "java-ce-events.basic-pubsub";
        String clientId = "kubemq-ce-java-example";

        BlockingQueue<String> received = new ArrayBlockingQueue<>(1);

        // Start SSE subscriber in background thread.
        String sseUrl = base + "/ce/subscribe/events?client_id=" + clientId
                + "-sub&channel=" + channel;
        Thread subscriber = Thread.ofVirtual().start(() -> {
            try {
                HttpURLConnection conn = (HttpURLConnection) new URL(sseUrl).openConnection();
                conn.setRequestMethod("GET");
                conn.setRequestProperty("Accept", "text/event-stream");
                conn.setRequestProperty("Cache-Control", "no-cache");
                conn.setDoInput(true);
                conn.setReadTimeout(15_000);

                try (BufferedReader reader = new BufferedReader(
                        new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
                    String line;
                    String eventType = null, data = null;
                    while ((line = reader.readLine()) != null) {
                        if (line.isEmpty()) {
                            if ("cloudevent".equals(eventType) && data != null) {
                                received.offer(data);
                                return;
                            }
                            eventType = null;
                            data = null;
                        } else if (line.startsWith(":")) {
                            // keepalive
                        } else if (line.startsWith("event:")) {
                            eventType = line.substring("event:".length()).trim();
                        } else if (line.startsWith("data:")) {
                            data = line.substring("data:".length()).trim();
                        }
                    }
                }
            } catch (Exception e) {
                System.err.println("SSE error: " + e.getMessage());
            }
        });

        // Allow subscription to establish.
        Thread.sleep(500);

        // Build CloudEvent (structured mode).
        EventFormatProvider.getInstance().registerFormat(new JsonFormat());
        EventFormat format = EventFormatProvider.getInstance().resolveFormat(JsonFormat.CONTENT_TYPE);

        CloudEvent event = CloudEventBuilder.v1()
                .withId(UUID.randomUUID().toString())
                .withType("com.kubemq.examples.events.sent")
                .withSource(URI.create(clientId))
                .withSubject(channel)
                .withDataContentType("application/json")
                .withTime(OffsetDateTime.now())
                .withData("application/json",
                        MAPPER.writeValueAsBytes(Map.of("message", "Hello from Java CloudEvents example!")))
                .build();

        byte[] body = format.serialize(event);

        HttpClient httpClient = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(base + "/ce/send/event"))
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .header("Content-Type", "application/cloudevents+json")
                .build();

        HttpResponse<String> response = httpClient.send(request,
                HttpResponse.BodyHandlers.ofString());
        Map<?, ?> result = MAPPER.readValue(response.body(), Map.class);
        System.out.printf("Published: status=%d is_error=%s%n",
                response.statusCode(), result.get("is_error"));

        // Wait for event.
        String data = received.poll(10, TimeUnit.SECONDS);
        if (data == null) {
            throw new RuntimeException("Timed out waiting for event");
        }
        Map<?, ?> ce = MAPPER.readValue(data, Map.class);
        System.out.println("Received event:");
        System.out.println("  type:    " + ce.get("type"));
        System.out.println("  source:  " + ce.get("source"));
        System.out.println("  subject: " + ce.get("subject"));
        System.out.println("  data:    " + ce.get("data"));
        subscriber.interrupt();
    }
}

// Expected output:
// Published: status=202 is_error=false
// Received event:
//   type:    com.kubemq.examples.events.sent
//   source:  kubemq-ce-java-example
//   subject: java-ce-events.basic-pubsub
//   data:    {message=Hello from Java CloudEvents example!}
