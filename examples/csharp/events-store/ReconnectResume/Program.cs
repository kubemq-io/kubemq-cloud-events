// Example: events-store/ReconnectResume
// Demonstrates Last-Event-ID reconnect. Receives 2 events, records last SSE id,
// reconnects without events_store_type (just Last-Event-ID header) to resume.
// Run: dotnet run
using CloudNative.CloudEvents;
using CloudNative.CloudEvents.SystemTextJson;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;

static string ServerUrl() => Environment.GetEnvironmentVariable("KUBEMQ_CE_URL") ?? "http://localhost:9090";

var base_ = ServerUrl();
var channel = "csharp-ce-events-store.reconnect-resume";
var formatter = new JsonEventFormatter();
using var httpClient = new HttpClient();

// Publish 4 events.
Console.WriteLine("Publishing 4 events...");
for (int i = 1; i <= 4; i++)
{
    var ev = new CloudEvent {
        Id = Guid.NewGuid().ToString(),
        Type = "com.kubemq.examples.eventsstore.stored",
        Source = new Uri("urn:kubemq-ce-csharp-example"),
        Subject = channel,
        DataContentType = "application/json",
        Data = new { seq = i },
    };
    var bytes = formatter.EncodeStructuredModeMessage(ev, out var ct);
    using var c = new ByteArrayContent(bytes.ToArray());
    c.Headers.ContentType = MediaTypeHeaderValue.Parse(ct.ToString());
    await httpClient.PostAsync($"{base_}/ce/send/event-store", c);
}

// Helper: subscribe and collect up to maxEvents, return (events, lastId).
async Task<(List<JsonElement> events, string lastId)> Subscribe(
    string clientId, string? lastEventId, int maxEvents)
{
    var events = new List<JsonElement>();
    var lastId = "";
    using var http = new HttpClient { Timeout = Timeout.InfiniteTimeSpan };
    // Omit events_store_type when reconnecting with Last-Event-ID.
    var url = lastEventId == null
        ? $"{base_}/ce/subscribe/events-store?client_id={clientId}&channel={Uri.EscapeDataString(channel)}&events_store_type=2"
        : $"{base_}/ce/subscribe/events-store?client_id={clientId}&channel={Uri.EscapeDataString(channel)}";
    using var req = new HttpRequestMessage(HttpMethod.Get, url);
    req.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("text/event-stream"));
    if (lastEventId != null) req.Headers.Add("Last-Event-ID", lastEventId);
    using var resp = await http.SendAsync(req, HttpCompletionOption.ResponseHeadersRead);
    using var reader = new StreamReader(await resp.Content.ReadAsStreamAsync(), Encoding.UTF8);
    string? evType = null, data = null, id = null, line;
    using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
    while ((line = await reader.ReadLineAsync().WaitAsync(cts.Token)) != null)
    {
        if (line == "") {
            if (evType == "cloudevent" && data != null) {
                if (id != null) lastId = id;
                events.Add(JsonSerializer.Deserialize<JsonElement>(data));
                if (events.Count >= maxEvents) break;
            }
            evType = null; data = null; id = null;
        }
        else if (line.StartsWith(":")) { }
        else if (line.StartsWith("id:")) id = line[3..].Trim();
        else if (line.StartsWith("event:")) evType = line[6..].Trim();
        else if (line.StartsWith("data:")) data = line[5..].Trim();
    }
    return (events, lastId);
}

// First connection: receive 2 events.
Console.WriteLine("First connection — receiving 2 events:");
var (first, lastId) = await Subscribe("csharp-es-reconnect-1", null, 2);
foreach (var ce in first)
    Console.WriteLine($"  Received: seq={ce.GetProperty("data").GetProperty("seq")}");
Console.WriteLine($"  Last-Event-ID recorded: {lastId}");

// Brief pause to allow broker to fully release the previous subscription.
await Task.Delay(500);

// Reconnect using Last-Event-ID with a new client_id (broker requires unique active client_id).
Console.WriteLine($"Reconnecting with Last-Event-ID={lastId}...");
try
{
    var (second, _) = await Subscribe("csharp-es-reconnect-2", lastId, 2);
    foreach (var ce in second)
        Console.WriteLine($"  Resumed: seq={ce.GetProperty("data").GetProperty("seq")}");
    if (second.Count == 0) Console.WriteLine("  (no events received on reconnect)");
}
catch (OperationCanceledException)
{
    Console.WriteLine("  Reconnect timed out waiting for events.");
}
Console.WriteLine("Reconnect-resume demonstration complete.");

// Expected output:
// Publishing 4 events...
// First connection — receiving 2 events:
//   Received: seq=1
//   Received: seq=2
//   Last-Event-ID recorded: 2
// Reconnecting with Last-Event-ID=2...
//   Resumed: seq=3
//   Resumed: seq=4
// Reconnect-resume demonstration complete.
