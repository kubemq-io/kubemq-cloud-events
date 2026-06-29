package io.kubemq.examples.queues.basicsendreceive;

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
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Example: queues/basic-send-receive
 *
 * Sends 3 CloudEvent messages to a queue, then receives them one at a time.
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
        String channel = "java-ce-queues.basic";
        String clientId = "kubemq-ce-java-worker";

        EventFormatProvider.getInstance().registerFormat(new JsonFormat());
        EventFormat format = EventFormatProvider.getInstance().resolveFormat(JsonFormat.CONTENT_TYPE);
        HttpClient httpClient = HttpClient.newHttpClient();

        // Send 3 messages.
        System.out.println("Sending 3 messages to queue '" + channel + "':");
        for (int i = 1; i <= 3; i++) {
            CloudEvent event = CloudEventBuilder.v1()
                    .withId(UUID.randomUUID().toString())
                    .withType("com.kubemq.examples.queues.task")
                    .withSource(URI.create("kubemq-ce-java-example"))
                    .withSubject(channel)
                    .withDataContentType("application/json")
                    .withTime(OffsetDateTime.now())
                    .withData("application/json",
                            MAPPER.writeValueAsBytes(Map.of("task_id", i, "task", "process-item")))
                    .build();
            HttpResponse<String> resp = httpClient.send(
                    HttpRequest.newBuilder().uri(URI.create(base + "/ce/queue/send"))
                            .POST(HttpRequest.BodyPublishers.ofByteArray(format.serialize(event)))
                            .header("Content-Type", "application/cloudevents+json").build(),
                    HttpResponse.BodyHandlers.ofString());
            Map<?, ?> r = MAPPER.readValue(resp.body(), Map.class);
            System.out.println("  Sent task " + i + ": is_error=" + r.get("is_error"));
        }

        // Receive messages one at a time.
        System.out.println("\nReceiving 3 messages:");
        for (int i = 0; i < 3; i++) {
            String url = base + "/ce/queue/receive?channel=" + channel
                    + "&client_id=" + clientId + "&max_messages=1&wait_timeout=5";
            HttpResponse<String> resp = httpClient.send(
                    HttpRequest.newBuilder().uri(URI.create(url))
                            .POST(HttpRequest.BodyPublishers.noBody()).build(),
                    HttpResponse.BodyHandlers.ofString());
            Map<?, ?> r = MAPPER.readValue(resp.body(), Map.class);
            if (Boolean.TRUE.equals(r.get("is_error"))) {
                System.out.println("  Error: " + r.get("message")); continue;
            }
            Map<?, ?> data = (Map<?, ?>) r.get("data");
            List<?> msgs = data != null ? (List<?>) data.get("messages") : List.of();
            if (msgs.isEmpty()) { System.out.println("  No messages"); continue; }
            for (Object m : msgs) {
                Map<?, ?> msg = (Map<?, ?>) m;
                System.out.println("  Received: type=" + msg.get("type") + " data=" + msg.get("data"));
            }
        }
    }
}

// Expected output:
// Sending 3 messages to queue 'java-ce-queues.basic':
//   Sent task 1: is_error=false
//   Sent task 2: is_error=false
//   Sent task 3: is_error=false
//
// Receiving 3 messages:
//   Received: type=com.kubemq.examples.queues.task data={task_id=1, task=process-item}
//   Received: type=com.kubemq.examples.queues.task data={task_id=2, task=process-item}
//   Received: type=com.kubemq.examples.queues.task data={task_id=3, task=process-item}
