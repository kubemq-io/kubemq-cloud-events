// Example: events-store/ReplayAtSequence
// Publishes 5 events, then subscribes with events_store_type=4 + events_store_value=3.
// Run: dotnet run
using CloudNative.CloudEvents;
using CloudNative.CloudEvents.SystemTextJson;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;

static string ServerUrl() => Environment.GetEnvironmentVariable("KUBEMQ_CE_URL") ?? "http://localhost:9090";

var base_ = ServerUrl();
var channel = "csharp-ce-events-store.replay-at-sequence";
var formatter = new JsonEventFormatter();
long startAtSeq = 3;
using var httpClient = new HttpClient();

// Publish 5 events.
Console.WriteLine("Publishing 5 events to events-store...");
for (int i = 1; i <= 5; i++)
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
Console.WriteLine($"Published 5 events. Subscribing from sequence {startAtSeq}...");

// events_store_type=4 + events_store_value=startAtSeq
var replayed = new System.Collections.Concurrent.ConcurrentQueue<JsonElement>();
var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
int expectedCount = (int)(5 - startAtSeq + 1);

var subTask = Task.Run(async () =>
{
    using var http = new HttpClient { Timeout = Timeout.InfiniteTimeSpan };
    var url = $"{base_}/ce/subscribe/events-store?client_id=csharp-es-seq&channel={Uri.EscapeDataString(channel)}&events_store_type=4&events_store_value={startAtSeq}";
    using var req = new HttpRequestMessage(HttpMethod.Get, url);
    req.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("text/event-stream"));
    using var resp = await http.SendAsync(req, HttpCompletionOption.ResponseHeadersRead);
    using var reader = new StreamReader(await resp.Content.ReadAsStreamAsync(), Encoding.UTF8);
    string? evType = null, data = null, line;
    while ((line = await reader.ReadLineAsync()) != null)
    {
        if (line == "") {
            if (evType == "cloudevent" && data != null) {
                replayed.Enqueue(JsonSerializer.Deserialize<JsonElement>(data));
                if (replayed.Count >= expectedCount) { done.TrySetResult(); return; }
            }
            evType = null; data = null;
        }
        else if (line.StartsWith("event:")) evType = line[6..].Trim();
        else if (line.StartsWith("data:")) data = line[5..].Trim();
    }
});

using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
await done.Task.WaitAsync(cts.Token);
Console.WriteLine($"Expecting {expectedCount} events from sequence {startAtSeq}:");
foreach (var ce in replayed)
    Console.WriteLine($"  Received: seq={ce.GetProperty("data").GetProperty("seq")}");

// Expected output:
// Publishing 5 events to events-store...
// Published 5 events. Subscribing from sequence 3...
// Expecting 3 events from sequence 3:
//   Received: seq=3
//   Received: seq=4
//   Received: seq=5
