package io.kubemq.examples.commands.roundtrip;

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
 * Example: commands/round-trip
 *
 * A responder subscribes to commands via SSE, receives the command, and sends
 * a CE response. A sender publishes a command via POST /ce/send/command
 * (which blocks until ack is received).
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
        String channel = "java-ce-commands.round-trip";

        EventFormatProvider.getInstance().registerFormat(new JsonFormat());
        EventFormat format = EventFormatProvider.getInstance().resolveFormat(JsonFormat.CONTENT_TYPE);
        HttpClient httpClient = HttpClient.newHttpClient();

        BlockingQueue<Boolean> responderReady = new ArrayBlockingQueue<>(1);

        // Start command responder.
        String sseUrl = base + "/ce/subscribe/commands?client_id=java-cmd-responder&channel=" + channel;
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
                                System.out.println("[responder] command received: type=" + raw.get("type")
                                        + " request_id=" + requestId);

                                CloudEvent respEvent = CloudEventBuilder.v1()
                                        .withId(UUID.randomUUID().toString())
                                        .withType("com.kubemq.examples.commands.response")
                                        .withSource(URI.create("kubemq-ce-java-responder"))
                                        .withSubject(replyChannel)
                                        .withDataContentType("application/json")
                                        .withTime(OffsetDateTime.now())
                                        .withData("application/json",
                                                MAPPER.writeValueAsBytes(Map.of("executed", true, "status", "command processed")))
                                        .build();
                                String responseUrl = base + "/ce/send/response?request_id=" + requestId;
                                HttpResponse<String> r = httpClient.send(
                                        HttpRequest.newBuilder().uri(URI.create(responseUrl))
                                                .POST(HttpRequest.BodyPublishers.ofByteArray(format.serialize(respEvent)))
                                                .header("Content-Type", "application/cloudevents+json").build(),
                                        HttpResponse.BodyHandlers.ofString());
                                Map<?, ?> rResult = MAPPER.readValue(r.body(), Map.class);
                                System.out.println("[responder] response sent: is_error=" + rResult.get("is_error"));
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

        // Send command.
        CloudEvent cmd = CloudEventBuilder.v1()
                .withId(UUID.randomUUID().toString())
                .withType("com.kubemq.examples.commands.reboot")
                .withSource(URI.create("kubemq-ce-java-sender"))
                .withSubject(channel)
                .withDataContentType("application/json")
                .withTime(OffsetDateTime.now())
                .withData("application/json",
                        MAPPER.writeValueAsBytes(Map.of("device_id", "sensor-42", "action", "reboot")))
                .build();

        System.out.println("[sender] sending command...");
        HttpResponse<String> resp = httpClient.send(
                HttpRequest.newBuilder().uri(URI.create(base + "/ce/send/command"))
                        .POST(HttpRequest.BodyPublishers.ofByteArray(format.serialize(cmd)))
                        .header("Content-Type", "application/cloudevents+json").build(),
                HttpResponse.BodyHandlers.ofString());
        Map<?, ?> result = MAPPER.readValue(resp.body(), Map.class);
        System.out.println("[sender] command ack: status=" + resp.statusCode() + " is_error=" + result.get("is_error"));
    }
}

// Expected output:
// [responder] command received: type=com.kubemq.examples.commands.reboot request_id=...
// [responder] response sent: is_error=false
// [sender] sending command...
// [sender] command ack: status=202 is_error=false
