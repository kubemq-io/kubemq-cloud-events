package io.kubemq.examples.eventsstore.reconnectresume;

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
import java.util.concurrent.atomic.AtomicReference;

/**
 * Example: events-store/reconnect-resume
 *
 * Demonstrates Last-Event-ID reconnect. Subscribes, receives one event and records
 * the SSE id field, then reconnects using Last-Event-ID header (no events_store_type
 * param) to resume from the next sequence.
 *
 * Run: mvn compile exec:java
 */
public class Main {
    static String serverUrl() {
        String u = System.getenv("KUBEMQ_CE_URL");
        return (u != null && !u.isEmpty()) ? u : "http://localhost:9090";
    }
    static final ObjectMapper MAPPER = new ObjectMapper();

    /** Opens one SSE connection, reads up to maxEvents, returns lastEventID seen. */
    static String subscribe(String base, String channel, String clientId,
                            String lastEventId, int maxEvents,
                            BlockingQueue<String> out) throws Exception {
        // Build URL — omit events_store_type when reconnecting with Last-Event-ID.
        String sseUrl = base + "/ce/subscribe/events-store?client_id=" + clientId + "&channel=" + channel;
        if (lastEventId == null) {
            // Initial connection: start from first stored message.
            sseUrl += "&events_store_type=2";
        }
        final String finalLastEventId = lastEventId;
        final String finalUrl = sseUrl;
        AtomicReference<String> lastId = new AtomicReference<>("");
        BlockingQueue<Boolean> done = new ArrayBlockingQueue<>(1);

        Thread.ofVirtual().start(() -> {
            try {
                HttpURLConnection conn = (HttpURLConnection) new URL(finalUrl).openConnection();
                conn.setRequestProperty("Accept", "text/event-stream");
                conn.setReadTimeout(10_000);
                if (finalLastEventId != null) {
                    conn.setRequestProperty("Last-Event-ID", finalLastEventId);
                }
                int[] count = {0};
                try (BufferedReader reader = new BufferedReader(
                        new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
                    String line; String evType = null, data = null, id = null;
                    while ((line = reader.readLine()) != null && count[0] < maxEvents) {
                        if (line.isEmpty()) {
                            if ("cloudevent".equals(evType) && data != null) {
                                if (id != null) lastId.set(id);
                                out.offer(data);
                                count[0]++;
                                if (count[0] >= maxEvents) break;
                            }
                            evType = null; data = null; id = null;
                        } else if (line.startsWith("id:")) id = line.substring(3).trim();
                        else if (line.startsWith("event:")) evType = line.substring(6).trim();
                        else if (line.startsWith("data:")) data = line.substring(5).trim();
                    }
                }
            } catch (Exception e) { /* read ended */ }
            done.offer(true);
        });
        done.poll(12, TimeUnit.SECONDS);
        return lastId.get();
    }

    public static void main(String[] args) throws Exception {
        String base = serverUrl();
        String channel = "java-ce-events-store.reconnect-resume";

        EventFormatProvider.getInstance().registerFormat(new JsonFormat());
        EventFormat format = EventFormatProvider.getInstance().resolveFormat(JsonFormat.CONTENT_TYPE);
        HttpClient httpClient = HttpClient.newHttpClient();

        // Publish 4 events.
        System.out.println("Publishing 4 events...");
        for (int i = 1; i <= 4; i++) {
            CloudEvent ev = CloudEventBuilder.v1()
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
                            .POST(HttpRequest.BodyPublishers.ofByteArray(format.serialize(ev)))
                            .header("Content-Type", "application/cloudevents+json").build(),
                    HttpResponse.BodyHandlers.ofString());
        }

        // First connection: receive 2 events, record lastEventID.
        BlockingQueue<String> received = new ArrayBlockingQueue<>(10);
        System.out.println("First connection — receiving 2 events:");
        String lastId = subscribe(base, channel, "java-es-reconnect", null, 2, received);
        for (int i = 0; i < 2; i++) {
            String data = received.poll(5, TimeUnit.SECONDS);
            if (data != null) {
                Map<?, ?> ce = MAPPER.readValue(data, Map.class);
                System.out.println("  Received: seq=" + ((Map<?, ?>) ce.get("data")).get("seq"));
            }
        }
        System.out.println("  Last-Event-ID recorded: " + lastId);

        // Reconnect using Last-Event-ID — resume from next sequence.
        System.out.println("Reconnecting with Last-Event-ID=" + lastId + "...");
        String lastId2 = subscribe(base, channel, "java-es-reconnect", lastId, 2, received);
        for (int i = 0; i < 2; i++) {
            String data = received.poll(5, TimeUnit.SECONDS);
            if (data != null) {
                Map<?, ?> ce = MAPPER.readValue(data, Map.class);
                System.out.println("  Resumed: seq=" + ((Map<?, ?>) ce.get("data")).get("seq"));
            }
        }
        System.out.println("Reconnect-resume demonstration complete.");
    }
}

// Expected output:
// Publishing 4 events...
// First connection — receiving 2 events:
//   Received: seq=1
//   Received: seq=2
//   Last-Event-ID recorded: 2
// Reconnecting with Last-Event-ID=2...
//   Resumed: seq=3
//   Resumed: seq=4
// Reconnect-resume demonstration complete.
