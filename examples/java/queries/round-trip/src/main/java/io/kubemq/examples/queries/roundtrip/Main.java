package io.kubemq.examples.queries.roundtrip;

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
 * Example: queries/round-trip
 *
 * A responder subscribes to queries, receives a query, looks up inventory,
 * and sends a CE response. A sender publishes a query via POST /ce/send/query
 * and prints the response returned directly in the HTTP response body.
 *
 * Run: mvn compile exec:java
 */
public class Main {
    static String serverUrl() {
        String u = System.getenv("KUBEMQ_CE_URL");
        return (u != null && !u.isEmpty()) ? u : "http://localhost:9090";
    }
    static final ObjectMapper MAPPER = new ObjectMapper();
    static final Map<String, Integer> INVENTORY = Map.of("WIDGET-100", 42, "GADGET-200", 7);

    public static void main(String[] args) throws Exception {
        String base = serverUrl();
        String channel = "java-ce-queries.round-trip";

        EventFormatProvider.getInstance().registerFormat(new JsonFormat());
        EventFormat format = EventFormatProvider.getInstance().resolveFormat(JsonFormat.CONTENT_TYPE);
        HttpClient httpClient = HttpClient.newHttpClient();

        BlockingQueue<Boolean> responderReady = new ArrayBlockingQueue<>(1);

        // Start query responder.
        String sseUrl = base + "/ce/subscribe/queries?client_id=java-query-responder&channel=" + channel;
        Thread.ofVirtual().start(() -> {
            try {
                HttpURLConnection conn = (HttpURLConnection) new URL(sseUrl).openConnection();
                conn.setRequestProperty("Accept", "text/event-stream");
                conn.setReadTimeout(30_000);
                try (BufferedReader reader = new BufferedReader(
                        new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
                    String line; String evType = null, data = null;
                    boolean ready = false;
                    while ((line = reader.readLine()) != null) {
                        if (!ready) { responderReady.offer(true); ready = true; }
                        if (line.isEmpty()) {
                            if ("cloudevent".equals(evType) && data != null) {
                                Map<?, ?> raw = MAPPER.readValue(data, Map.class);
                                String requestId = (String) raw.get("_kubemq_request_id");
                                String replyChannel = (String) raw.get("_kubemq_reply_channel");
                                Map<?, ?> qdata = (Map<?, ?>) raw.get("data");
                                String sku = qdata != null ? (String) qdata.get("sku") : "";
                                int qty = INVENTORY.getOrDefault(sku, 0);
                                System.out.println("[responder] query sku=" + sku + " qty=" + qty);

                                CloudEvent respEvent = CloudEventBuilder.v1()
                                        .withId(UUID.randomUUID().toString())
                                        .withType("com.kubemq.examples.queries.inventory-result")
                                        .withSource(URI.create("kubemq-ce-java-responder"))
                                        .withSubject(replyChannel)
                                        .withDataContentType("application/json")
                                        .withTime(OffsetDateTime.now())
                                        .withData("application/json",
                                                MAPPER.writeValueAsBytes(Map.of("sku", sku, "quantity", qty)))
                                        .build();
                                String responseUrl = base + "/ce/send/response?request_id=" + requestId;
                                httpClient.send(
                                        HttpRequest.newBuilder().uri(URI.create(responseUrl))
                                                .POST(HttpRequest.BodyPublishers.ofByteArray(format.serialize(respEvent)))
                                                .header("Content-Type", "application/cloudevents+json").build(),
                                        HttpResponse.BodyHandlers.ofString());
                                System.out.println("[responder] response sent.");
                                return;
                            }
                            evType = null; data = null;
                        } else if (line.startsWith("event:")) evType = line.substring(6).trim();
                        else if (line.startsWith("data:")) data = line.substring(5).trim();
                    }
                }
            } catch (Exception e) { System.err.println("SSE error: " + e.getMessage()); }
        });

        responderReady.poll(5, TimeUnit.SECONDS);
        Thread.sleep(100);

        // Send query.
        CloudEvent query = CloudEventBuilder.v1()
                .withId(UUID.randomUUID().toString())
                .withType("com.kubemq.examples.queries.inventory-check")
                .withSource(URI.create("kubemq-ce-java-sender"))
                .withSubject(channel)
                .withDataContentType("application/json")
                .withTime(OffsetDateTime.now())
                .withData("application/json", MAPPER.writeValueAsBytes(Map.of("sku", "WIDGET-100")))
                .build();

        System.out.println("[sender] sending query for sku=WIDGET-100...");
        HttpResponse<String> resp = httpClient.send(
                HttpRequest.newBuilder().uri(URI.create(base + "/ce/send/query"))
                        .POST(HttpRequest.BodyPublishers.ofByteArray(format.serialize(query)))
                        .header("Content-Type", "application/cloudevents+json").build(),
                HttpResponse.BodyHandlers.ofString());

        Map<?, ?> result = MAPPER.readValue(resp.body(), Map.class);
        System.out.println("[sender] query response: status=" + resp.statusCode() + " is_error=" + result.get("is_error"));
        System.out.println("[sender] response data: " + result.get("data"));
    }
}

// Expected output:
// [responder] query sku=WIDGET-100 qty=42
// [responder] response sent.
// [sender] sending query for sku=WIDGET-100...
// [sender] query response: status=200 is_error=false
// [sender] response data: {sku=WIDGET-100, quantity=42}
