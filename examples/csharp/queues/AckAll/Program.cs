// Example: queues/AckAll — drain queue atomically with ack_all.
// Run: dotnet run
using CloudNative.CloudEvents;
using CloudNative.CloudEvents.SystemTextJson;
using System.Net.Http.Headers;
using System.Text.Json;

static string ServerUrl() => Environment.GetEnvironmentVariable("KUBEMQ_CE_URL") ?? "http://localhost:9090";

var base_ = ServerUrl();
var channel = "csharp-ce-queues.ack-all";
var clientId = "kubemq-ce-csharp-example";
var formatter = new JsonEventFormatter();
using var httpClient = new HttpClient();

// Send 5 messages.
Console.WriteLine("Sending 5 messages to queue...");
for (int i = 1; i <= 5; i++)
{
    var ev = new CloudEvent {
        Id = Guid.NewGuid().ToString(),
        Type = "com.kubemq.examples.queues.ackall",
        Source = new Uri($"urn:{clientId}"),
        Subject = channel,
        DataContentType = "application/json",
        Data = new { n = i },
    };
    var bytes = formatter.EncodeStructuredModeMessage(ev, out var ct);
    using var c = new ByteArrayContent(bytes.ToArray());
    c.Headers.ContentType = MediaTypeHeaderValue.Parse(ct.ToString());
    await httpClient.PostAsync($"{base_}/ce/queue/send", c);
}

// Peek count helper.
async Task<int> PeekCount()
{
    var url = $"{base_}/ce/queue/receive?channel={Uri.EscapeDataString(channel)}&client_id={clientId}&max_messages=100&wait_timeout=3&is_peek=true";
    var r = await httpClient.PostAsync(url, null);
    var j = JsonSerializer.Deserialize<JsonElement>(await r.Content.ReadAsStringAsync());
    return j.TryGetProperty("data", out var d) && d.TryGetProperty("messages_received", out var n)
        ? n.GetInt32() : 0;
}

Console.WriteLine($"Peek before ack_all: {await PeekCount()} messages");

// ack_all.
var ackUrl = $"{base_}/ce/queue/ack_all?channel={Uri.EscapeDataString(channel)}&client_id={clientId}&wait_timeout=5";
var ackResp = await httpClient.PostAsync(ackUrl, null);
var ackJ = JsonSerializer.Deserialize<JsonElement>(await ackResp.Content.ReadAsStringAsync());
Console.WriteLine($"ack_all: is_error={ackJ.GetProperty("is_error")} message={ackJ.GetProperty("message")}");

Console.WriteLine($"Peek after ack_all: {await PeekCount()} messages remaining");

// Expected output:
// Sending 5 messages to queue...
// Peek before ack_all: 5 messages
// ack_all: is_error=False message=OK
// Peek after ack_all: 0 messages remaining
