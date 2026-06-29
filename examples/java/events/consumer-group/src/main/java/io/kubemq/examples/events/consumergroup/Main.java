package io.kubemq.examples.events.consumergroup;

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

public class Main {
    static String serverUrl() {
        String u = System.getenv("KUBEMQ_CE_URL");
        return (u != null && !u.isEmpty()) ? u : "http://localhost:9090";
    }
    static final ObjectMapper MAPPER = new ObjectMapper();

    static void subscribeGroup(String base, String channel, String group, String clientId,
                                BlockingQueue<String> results) {
        String url = base + "/ce/subscribe/events?client_id=" + clientId
                + "&channel=" + channel + "&group=" + group;
        Thread.ofVirtual().start(() -> {
            try {
                HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
                conn.setRequestProperty("Accept", "text/event-stream");
                conn.setReadTimeout(15_000);
                try (BufferedReader reader = new BufferedReader(
                        new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
                    String line;
                    String evType = null, data = null;
                    while ((line = reader.readLine()) != null) {
                        if (line.isEmpty()) {
                            if ("cloudevent".equals(evType) && data != null) {
                                results.offer("[" + clientId + "] received: " + data.substring(0, Math.min(80, data.length())));
                                return;
                            }
                            evType = null; data = null;
                        } else if (line.startsWith("event:")) evType = line.substring(6).trim();
                        else if (line.startsWith("data:")) data = line.substring(5).trim();
                    }
                }
            } catch (Exception e) { /* ignored */ }
        });
    }

    public static void main(String[] args) throws Exception {
        String base = serverUrl();
        String channel = "java-ce-events.consumer-group";
        BlockingQueue<String> results = new ArrayBlockingQueue<>(2);

        subscribeGroup(base, channel, "workers", "java-worker-1", results);
        subscribeGroup(base, channel, "workers", "java-worker-2", results);
        Thread.sleep(600);

        EventFormatProvider.getInstance().registerFormat(new JsonFormat());
        EventFormat format = EventFormatProvider.getInstance().resolveFormat(JsonFormat.CONTENT_TYPE);
        HttpClient httpClient = HttpClient.newHttpClient();

        for (int seq = 1; seq <= 2; seq++) {
            CloudEvent event = CloudEventBuilder.v1()
                    .withId(UUID.randomUUID().toString())
                    .withType("com.kubemq.examples.events.grouped")
                    .withSource(URI.create("kubemq-ce-java-example"))
                    .withSubject(channel)
                    .withDataContentType("application/json")
                    .withTime(OffsetDateTime.now())
                    .withData("application/json", MAPPER.writeValueAsBytes(Map.of("seq", seq)))
                    .build();
            byte[] body = format.serialize(event);
            HttpResponse<String> resp = httpClient.send(
                    HttpRequest.newBuilder().uri(URI.create(base + "/ce/send/event"))
                            .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                            .header("Content-Type", "application/cloudevents+json").build(),
                    HttpResponse.BodyHandlers.ofString());
            System.out.println("Published event seq=" + seq + " (status=" + resp.statusCode() + ")");
        }

        for (int i = 0; i < 2; i++) {
            String msg = results.poll(10, TimeUnit.SECONDS);
            if (msg != null) System.out.println(msg);
        }
    }
}
