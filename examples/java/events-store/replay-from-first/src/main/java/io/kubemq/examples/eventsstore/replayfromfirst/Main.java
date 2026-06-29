package io.kubemq.examples.eventsstore.replayfromfirst;

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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Example: events-store/replay-from-first
 *
 * Publishes 3 events to events-store, then subscribes with
 * events_store_type=2 (StartFromFirst) to replay all stored events.
 *
 * Run: mvn compile exec:java
 */
public class Main {
    static String serverUrl() {
        String u = System.getenv("KUBEMQ_CE_URL");
        return (u != null && !u.isEmpty()) ? u : "http://localhost:9090";
    }
    static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        String base = serverUrl();
        String channel = "java-ce-events-store.replay-from-first";

        EventFormatProvider.getInstance().registerFormat(new JsonFormat());
        EventFormat format = EventFormatProvider.getInstance().resolveFormat(JsonFormat.CONTENT_TYPE);
        HttpClient httpClient = HttpClient.newHttpClient();

        // Publish 3 events to events-store first.
        System.out.println("Publishing 3 events to events-store...");
        for (int i = 1; i <= 3; i++) {
            CloudEvent event = CloudEventBuilder.v1()
                    .withId(UUID.randomUUID().toString())
                    .withType("com.kubemq.examples.eventsstore.stored")
                    .withSource(URI.create("kubemq-ce-java-example"))
                    .withSubject(channel)
                    .withDataContentType("application/json")
                    .withTime(OffsetDateTime.now())
                    .withData("application/json", MAPPER.writeValueAsBytes(Map.of("seq", i)))
                    .build();
            httpClient.send(
                    HttpRequest.newBuilder().uri(URI.create(base + "/ce/send/event-store"))
                            .POST(HttpRequest.BodyPublishers.ofByteArray(format.serialize(event)))
                            .header("Content-Type", "application/cloudevents+json").build(),
                    HttpResponse.BodyHandlers.ofString());
            System.out.println("  Published seq=" + i);
        }

        // Subscribe with StartFromFirst (events_store_type=2) to replay all 3.
        BlockingQueue<String> received = new ArrayBlockingQueue<>(10);
        // events_store_type=2 = StartFromFirst
        String sseUrl = base + "/ce/subscribe/events-store?client_id=java-es-replay&channel="
                + channel + "&events_store_type=2";
        Thread.ofVirtual().start(() -> {
            try {
                HttpURLConnection conn = (HttpURLConnection) new URL(sseUrl).openConnection();
                conn.setRequestProperty("Accept", "text/event-stream");
                conn.setReadTimeout(15_000);
                try (BufferedReader reader = new BufferedReader(
                        new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
                    String line; String evType = null, data = null;
                    while ((line = reader.readLine()) != null) {
                        if (line.isEmpty()) {
                            if ("cloudevent".equals(evType) && data != null) {
                                received.offer(data);
                            } else if ("error".equals(evType) && data != null) {
                                System.err.println("SSE error: " + data);
                                return;
                            }
                            evType = null; data = null;
                        } else if (line.startsWith("event:")) evType = line.substring(6).trim();
                        else if (line.startsWith("data:")) data = line.substring(5).trim();
                    }
                }
            } catch (Exception e) { System.err.println("SSE error: " + e.getMessage()); }
        });

        // Collect up to 3 replayed events.
        List<Map<?, ?>> events = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            String data = received.poll(10, TimeUnit.SECONDS);
            if (data == null) { System.err.println("Timed out waiting for event " + (i + 1)); break; }
            events.add(MAPPER.readValue(data, Map.class));
        }

        System.out.println("Replayed " + events.size() + " events from first:");
        for (Map<?, ?> ce : events) {
            System.out.println("  seq=" + ((Map<?, ?>) ce.get("data")).get("seq") + " id=" + ce.get("id"));
        }
    }
}

// Expected output:
// Publishing 3 events to events-store...
//   Published seq=1
//   Published seq=2
//   Published seq=3
// Replayed 3 events from first:
//   seq=1 id=...
//   seq=2 id=...
//   seq=3 id=...
