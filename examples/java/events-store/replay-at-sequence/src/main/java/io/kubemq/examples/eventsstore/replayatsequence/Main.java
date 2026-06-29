package io.kubemq.examples.eventsstore.replayatsequence;

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

/**
 * Example: events-store/replay-at-sequence
 *
 * Publishes 5 events, then subscribes with events_store_type=4 (StartAtSequence)
 * and events_store_value=3 to replay from sequence 3 onward.
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
        String channel = "java-ce-events-store.replay-at-sequence";
        long startAtSeq = 3;

        EventFormatProvider.getInstance().registerFormat(new JsonFormat());
        EventFormat format = EventFormatProvider.getInstance().resolveFormat(JsonFormat.CONTENT_TYPE);
        HttpClient httpClient = HttpClient.newHttpClient();

        // Publish 5 events.
        System.out.println("Publishing 5 events to events-store...");
        for (int i = 1; i <= 5; i++) {
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
        }
        System.out.println("Published 5 events. Subscribing from sequence " + startAtSeq + "...");

        // events_store_type=4 = StartAtSequence, events_store_value=startAtSeq
        BlockingQueue<String> received = new ArrayBlockingQueue<>(10);
        String sseUrl = base + "/ce/subscribe/events-store?client_id=java-es-seq&channel="
                + channel + "&events_store_type=4&events_store_value=" + startAtSeq;
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
                            if ("cloudevent".equals(evType) && data != null) received.offer(data);
                            else if ("error".equals(evType)) { System.err.println("SSE error: " + data); return; }
                            evType = null; data = null;
                        } else if (line.startsWith("event:")) evType = line.substring(6).trim();
                        else if (line.startsWith("data:")) data = line.substring(5).trim();
                    }
                }
            } catch (Exception e) { System.err.println("SSE error: " + e.getMessage()); }
        });

        // Expect events from sequence startAtSeq onward (3 events: seq 3,4,5).
        int expectedCount = (int)(5 - startAtSeq + 1);
        System.out.println("Expecting " + expectedCount + " events from sequence " + startAtSeq + ":");
        for (int i = 0; i < expectedCount; i++) {
            String data = received.poll(10, TimeUnit.SECONDS);
            if (data == null) { System.err.println("Timed out"); break; }
            Map<?, ?> ce = MAPPER.readValue(data, Map.class);
            System.out.println("  Received: seq=" + ((Map<?, ?>) ce.get("data")).get("seq"));
        }
    }
}

// Expected output:
// Publishing 5 events to events-store...
// Published 5 events. Subscribing from sequence 3...
// Expecting 3 events from sequence 3:
//   Received: seq=3
//   Received: seq=4
//   Received: seq=5
