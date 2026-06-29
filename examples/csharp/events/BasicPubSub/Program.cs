// Example: events/BasicPubSub
//
// Demonstrates basic fire-and-forget event pub/sub via the KubeMQ
// CloudEvents HTTP connector.
//
// Run: dotnet run

using CloudNative.CloudEvents;
using CloudNative.CloudEvents.SystemTextJson;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;

static string ServerUrl() =>
    Environment.GetEnvironmentVariable("KUBEMQ_CE_URL") ?? "http://localhost:9090";

var base_ = ServerUrl();
var channel = "csharp-ce-events.basic-pubsub";
var clientId = "kubemq-ce-csharp-example";

var received = new TaskCompletionSource<JsonElement>(
    TaskCreationOptions.RunContinuationsAsynchronously);

// Start SSE subscriber.
var subscriberTask = Task.Run(async () =>
{
    using var httpClient = new HttpClient { Timeout = Timeout.InfiniteTimeSpan };
    var sseUrl = $"{base_}/ce/subscribe/events?client_id={clientId}-sub&channel={Uri.EscapeDataString(channel)}";
    using var request = new HttpRequestMessage(HttpMethod.Get, sseUrl);
    request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("text/event-stream"));
    request.Headers.CacheControl = new CacheControlHeaderValue { NoCache = true };

    using var response = await httpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead);
    using var stream = await response.Content.ReadAsStreamAsync();
    using var reader = new StreamReader(stream, Encoding.UTF8);

    string? eventType = null, data = null;
    string? line;
    while ((line = await reader.ReadLineAsync()) != null)
    {
        if (line == "")
        {
            if (eventType == "cloudevent" && data != null)
            {
                received.TrySetResult(JsonSerializer.Deserialize<JsonElement>(data));
                return;
            }
            if (eventType == "error")
            {
                Console.Error.WriteLine($"SSE error: {data}");
                return;
            }
            eventType = null; data = null;
        }
        else if (line.StartsWith(":")) { /* keepalive */ }
        else if (line.StartsWith("event:")) eventType = line["event:".Length..].Trim();
        else if (line.StartsWith("data:")) data = line["data:".Length..].Trim();
    }
});

// Allow subscription to establish.
await Task.Delay(500);

// Build and publish CloudEvent (structured mode).
var formatter = new JsonEventFormatter();
var cloudEvent = new CloudEvent
{
    Id = Guid.NewGuid().ToString(),
    Type = "com.kubemq.examples.events.sent",
    Source = new Uri($"urn:{clientId}"),
    Subject = channel,
    DataContentType = "application/json",
    Data = new { message = "Hello from C# CloudEvents example!" },
};
cloudEvent.SetAttributeFromString("time", DateTimeOffset.UtcNow.ToString("O"));

var eventBytes = formatter.EncodeStructuredModeMessage(cloudEvent, out var contentType);
using var httpClient = new HttpClient();
using var content = new ByteArrayContent(eventBytes.ToArray());
content.Headers.ContentType = MediaTypeHeaderValue.Parse(contentType.ToString());

var resp = await httpClient.PostAsync($"{base_}/ce/send/event", content);
var resultJson = await resp.Content.ReadAsStringAsync();
using var resultDoc = JsonDocument.Parse(resultJson);
Console.WriteLine($"Published: status={resp.StatusCode} is_error={resultDoc.RootElement.GetProperty("is_error")}");

// Wait for the event.
using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
try
{
    var ce = await received.Task.WaitAsync(cts.Token);
    Console.WriteLine("Received event:");
    Console.WriteLine($"  type:    {ce.GetProperty("type")}");
    Console.WriteLine($"  source:  {ce.GetProperty("source")}");
    Console.WriteLine($"  subject: {ce.GetProperty("subject")}");
    Console.WriteLine($"  data:    {ce.GetProperty("data")}");
}
catch (OperationCanceledException)
{
    Console.Error.WriteLine("Timed out waiting for event");
}

// Expected output:
// Published: status=Accepted is_error=false
// Received event:
//   type:    com.kubemq.examples.events.sent
//   source:  urn:kubemq-ce-csharp-example
//   subject: csharp-ce-events.basic-pubsub
//   data:    {"message":"Hello from C# CloudEvents example!"}
