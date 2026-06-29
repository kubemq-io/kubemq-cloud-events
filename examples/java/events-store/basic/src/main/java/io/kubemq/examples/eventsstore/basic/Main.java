package io.kubemq.examples.eventsstore.basic;

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
        String channel = "java-ce-events-store.basic";
        BlockingQueue<String> received = new ArrayBlockingQueue<>(1);

        String sseUrl = base + "/ce/subscribe/events-store?client_id=java-es-sub&channel="
                + channel + "&events_store_type=1";
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
                            if ("cloudevent".equals(evType) && data != null) { received.offer(data); return; }
                            evType = null; data = null;
                        } else if (line.startsWith("event:")) evType = line.substring(6).trim();
                        else if (line.startsWith("data:")) data = line.substring(5).trim();
                    }
                }
            } catch (Exception e) { System.err.println("SSE error: " + e.getMessage()); }
        });

        Thread.sleep(500);

        EventFormatProvider.getInstance().registerFormat(new JsonFormat());
        EventFormat format = EventFormatProvider.getInstance().resolveFormat(JsonFormat.CONTENT_TYPE);
        CloudEvent event = CloudEventBuilder.v1()
                .withId(UUID.randomUUID().toString())
                .withType("com.kubemq.examples.eventsstore.stored")
                .withSource(URI.create("kubemq-ce-java-example"))
                .withSubject(channel)
                .withDataContentType("application/json")
                .withTime(OffsetDateTime.now())
                .withData("application/json",
                        MAPPER.writeValueAsBytes(Map.of("msg", "hello events-store from Java!")))
                .build();

        HttpClient httpClient = HttpClient.newHttpClient();
        HttpResponse<String> resp = httpClient.send(
                HttpRequest.newBuilder().uri(URI.create(base + "/ce/send/event-store"))
                        .POST(HttpRequest.BodyPublishers.ofByteArray(format.serialize(event)))
                        .header("Content-Type", "application/cloudevents+json").build(),
                HttpResponse.BodyHandlers.ofString());
        System.out.println("Published to events-store: status=" + resp.statusCode());

        String data = received.poll(10, TimeUnit.SECONDS);
        if (data == null) throw new RuntimeException("Timed out");
        Map<?, ?> ce = MAPPER.readValue(data, Map.class);
        System.out.println("Received: type=" + ce.get("type") + " data=" + ce.get("data"));
    }
}
