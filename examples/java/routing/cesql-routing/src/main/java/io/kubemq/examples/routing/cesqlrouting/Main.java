package io.kubemq.examples.routing.cesqlrouting;

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
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Example: routing/cesql-routing
 *
 * Demonstrates server-side CESQL routing. Events with different type values
 * are published to the source channel. KubeMQ routes them based on CESQL
 * expressions to different destination channels.
 *
 * Requires KubeMQ configured with CESQL routing rules, e.g.:
 * [Routing]
 *   Enable = true
 *   Data = '[{"key":"type = ''com.kubemq.examples.routing.order''","keyType":"cesql","routes":"events:order-archive"},{"key":"type = ''com.kubemq.examples.routing.alert''","keyType":"cesql","routes":"events:alert-stream"},{"key":"type LIKE ''com.kubemq.examples.%''","keyType":"cesql","routes":"events:all-events"}]'
 *
 * Run: mvn compile exec:java
 */
public class Main {
    static String serverUrl() {
        String u = System.getenv("KUBEMQ_CE_URL");
        return (u != null && !u.isEmpty()) ? u : "http://localhost:9090";
    }
    static final ObjectMapper MAPPER = new ObjectMapper();

    static void startSubscriber(String base, String channel, String clientId,
                                 int maxEvents, BlockingQueue<String> results) {
        String sseUrl = base + "/ce/subscribe/events?client_id=" + clientId + "&channel=" + channel;
        Thread.ofVirtual().start(() -> {
            try {
                HttpURLConnection conn = (HttpURLConnection) new URL(sseUrl).openConnection();
                conn.setRequestProperty("Accept", "text/event-stream");
                conn.setReadTimeout(15_000);
                try (BufferedReader reader = new BufferedReader(
                        new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
                    String line; String evType = null, data = null; int count = 0;
                    while ((line = reader.readLine()) != null && count < maxEvents) {
                        if (line.isEmpty()) {
                            if ("cloudevent".equals(evType) && data != null) {
                                Map<?, ?> ce = MAPPER.readValue(data, Map.class);
                                results.offer("  [" + channel + "] type=" + ce.get("type"));
                                count++;
                            }
                            evType = null; data = null;
                        } else if (line.startsWith("event:")) evType = line.substring(6).trim();
                        else if (line.startsWith("data:")) data = line.substring(5).trim();
                    }
                }
            } catch (Exception e) { /* read ended */ }
        });
    }

    public static void main(String[] args) throws Exception {
        String base = serverUrl();
        System.out.println("CESQL Routing Example — Java");
        System.out.println("Requires KubeMQ with CESQL routing configured.\n");

        EventFormatProvider.getInstance().registerFormat(new JsonFormat());
        EventFormat format = EventFormatProvider.getInstance().resolveFormat(JsonFormat.CONTENT_TYPE);
        HttpClient httpClient = HttpClient.newHttpClient();

        // Subscribe to routed destination channels.
        BlockingQueue<String> results = new ArrayBlockingQueue<>(20);
        startSubscriber(base, "order-archive", "java-order-sub", 1, results);
        startSubscriber(base, "alert-stream",  "java-alert-sub", 1, results);
        startSubscriber(base, "all-events",    "java-all-sub",   3, results);
        Thread.sleep(500);

        // Publish events with different type values; CESQL rules route them.
        List<String> eventTypes = List.of(
                "com.kubemq.examples.routing.order",
                "com.kubemq.examples.routing.alert",
                "com.kubemq.examples.routing.info"
        );
        for (String evType : eventTypes) {
            CloudEvent event = CloudEventBuilder.v1()
                    .withId(UUID.randomUUID().toString())
                    .withType(evType)
                    .withSource(URI.create("kubemq-ce-java-cesql"))
                    .withSubject("routing-source")
                    .withDataContentType("application/json")
                    .withTime(OffsetDateTime.now())
                    .withData("application/json",
                            MAPPER.writeValueAsBytes(Map.of("description", "Event of type " + evType)))
                    .build();
            HttpResponse<String> resp = httpClient.send(
                    HttpRequest.newBuilder().uri(URI.create(base + "/ce/send/event"))
                            .POST(HttpRequest.BodyPublishers.ofByteArray(format.serialize(event)))
                            .header("Content-Type", "application/cloudevents+json").build(),
                    HttpResponse.BodyHandlers.ofString());
            System.out.println("Published type=" + evType + " (status=" + resp.statusCode() + ")");
        }

        // Collect routed events (up to 5 with timeout).
        Thread.sleep(2000);
        for (int i = 0; i < 5; i++) {
            String msg = results.poll(100, TimeUnit.MILLISECONDS);
            if (msg != null) System.out.println(msg);
        }
        System.out.println("\nCESQL routing demonstration complete.");
    }
}

// Expected output (with CESQL routing configured):
// CESQL Routing Example — Java
// Requires KubeMQ with CESQL routing configured.
//
// Published type=com.kubemq.examples.routing.order (status=202)
// Published type=com.kubemq.examples.routing.alert (status=202)
// Published type=com.kubemq.examples.routing.info (status=202)
//   [order-archive] type=com.kubemq.examples.routing.order
//   [alert-stream] type=com.kubemq.examples.routing.alert
//   [all-events] type=com.kubemq.examples.routing.order
//   [all-events] type=com.kubemq.examples.routing.alert
//   [all-events] type=com.kubemq.examples.routing.info
//
// CESQL routing demonstration complete.
