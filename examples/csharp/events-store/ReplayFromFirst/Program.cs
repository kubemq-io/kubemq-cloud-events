// Example: events-store/ReplayFromFirst
// Publishes 3 events, then subscribes with events_store_type=2 (StartFromFirst).
// Run: dotnet run
using CloudNative.CloudEvents;
using CloudNative.CloudEvents.SystemTextJson;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;

static string ServerUrl() => Environment.GetEnvironmentVariable("KUBEMQ_CE_URL") ?? "http://localhost:9090";

var base_ = ServerUrl();
var channel = "csharp-ce-events-store.replay-from-first";
var formatter = new JsonEventFormatter();
using var httpClient = new HttpClient();

// Publish 3 events first.
Console.WriteLine("Publishing 3 events to events-store...");
for (int i = 1; i <= 3; i++)
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
    Console.WriteLine($"  Published seq={i}");
}

// Subscribe with StartFromFirst (events_store_type=2).
var replayed = new System.Collections.Concurrent.ConcurrentQueue<JsonElement>();
var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

var subTask = Task.Run(async () =>
{
    using var http = new HttpClient { Timeout = Timeout.InfiniteTimeSpan };
    var url = $"{base_}/ce/subscribe/events-store?client_id=csharp-es-replay&channel={Uri.EscapeDataString(channel)}&events_store_type=2";
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
                if (replayed.Count >= 3) { done.TrySetResult(); return; }
            }
            evType = null; data = null;
        }
        else if (line.StartsWith("event:")) evType = line[6..].Trim();
        else if (line.StartsWith("data:")) data = line[5..].Trim();
    }
});

using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
await done.Task.WaitAsync(cts.Token);
Console.WriteLine($"Replayed {replayed.Count} events from first:");
foreach (var ce in replayed)
    Console.WriteLine($"  seq={ce.GetProperty("data").GetProperty("seq")} id={ce.GetProperty("id")}");

// Expected output:
// Publishing 3 events to events-store...
//   Published seq=1
//   Published seq=2
//   Published seq=3
// Replayed 3 events from first:
//   seq=1 id=...
//   seq=2 id=...
//   seq=3 id=...
