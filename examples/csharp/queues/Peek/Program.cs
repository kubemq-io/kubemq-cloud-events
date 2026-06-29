// Example: queues/Peek — inspect queue without consuming (is_peek=true).
// Run: dotnet run
using CloudNative.CloudEvents;
using CloudNative.CloudEvents.SystemTextJson;
using System.Net.Http.Headers;
using System.Text.Json;

static string ServerUrl() => Environment.GetEnvironmentVariable("KUBEMQ_CE_URL") ?? "http://localhost:9090";

var base_ = ServerUrl();
var channel = "csharp-ce-queues.peek";
var clientId = "kubemq-ce-csharp-worker";
var formatter = new JsonEventFormatter();
using var httpClient = new HttpClient();

// Send 3 messages.
Console.WriteLine("Sending 3 messages to queue...");
for (int i = 1; i <= 3; i++)
{
    var ev = new CloudEvent {
        Id = Guid.NewGuid().ToString(),
        Type = "com.kubemq.examples.queues.peek",
        Source = new Uri("urn:kubemq-ce-csharp-example"),
        Subject = channel,
        DataContentType = "application/json",
        Data = new { n = i },
    };
    var bytes = formatter.EncodeStructuredModeMessage(ev, out var ct);
    using var c = new ByteArrayContent(bytes.ToArray());
    c.Headers.ContentType = MediaTypeHeaderValue.Parse(ct.ToString());
    var r = await httpClient.PostAsync($"{base_}/ce/queue/send", c);
    Console.WriteLine($"  Sent message {i}: status={r.StatusCode}");
}

// Helper: peek or receive.
async Task<int> PeekOrReceive(bool isPeak, string label)
{
    var url = $"{base_}/ce/queue/receive?channel={Uri.EscapeDataString(channel)}&client_id={clientId}&max_messages=10&wait_timeout=3&is_peek={isPeak.ToString().ToLower()}";
    var r = await httpClient.PostAsync(url, null);
    var j = JsonSerializer.Deserialize<JsonElement>(await r.Content.ReadAsStringAsync());
    int count = 0;
    if (j.TryGetProperty("data", out var data) && data.TryGetProperty("messages_received", out var n))
        count = n.GetInt32();
    Console.WriteLine($"[{label}] messages_received={count}");
    return count;
}

await PeekOrReceive(true,  "peek #1");
await PeekOrReceive(true,  "peek #2");
await PeekOrReceive(false, "consume");

// Expected output:
// Sending 3 messages to queue...
//   Sent message 1: status=Accepted
//   Sent message 2: status=Accepted
//   Sent message 3: status=Accepted
// [peek #1] messages_received=3
// [peek #2] messages_received=3
// [consume] messages_received=3
