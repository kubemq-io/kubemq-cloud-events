// Example: routing/CesqlRouting — publish events for server-side CESQL routing.
// Requires KubeMQ configured with CESQL rules (see Go example for config).
// Run: dotnet run
using CloudNative.CloudEvents;
using CloudNative.CloudEvents.SystemTextJson;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;

static string ServerUrl() => Environment.GetEnvironmentVariable("KUBEMQ_CE_URL") ?? "http://localhost:9090";

var base_ = ServerUrl();
var formatter = new JsonEventFormatter();
Console.WriteLine("CESQL Routing Example — C#");
Console.WriteLine("Requires KubeMQ with CESQL routing configured.\n");

var results = new System.Collections.Concurrent.ConcurrentQueue<string>();

// Subscribe to routed destination channels.
async Task StartSubscriber(string ch, string clientId, int maxEvents)
{
    using var http = new HttpClient { Timeout = Timeout.InfiniteTimeSpan };
    var url = $"{base_}/ce/subscribe/events?client_id={clientId}&channel={Uri.EscapeDataString(ch)}";
    using var req = new HttpRequestMessage(HttpMethod.Get, url);
    req.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("text/event-stream"));
    using var resp = await http.SendAsync(req, HttpCompletionOption.ResponseHeadersRead);
    using var reader = new StreamReader(await resp.Content.ReadAsStreamAsync(), Encoding.UTF8);
    string? evType = null, data = null, line; int count = 0;
    while ((line = await reader.ReadLineAsync()) != null && count < maxEvents)
    {
        if (line == "") {
            if (evType == "cloudevent" && data != null) {
                var ce = JsonSerializer.Deserialize<JsonElement>(data);
                results.Enqueue($"  [{ch}] type={ce.GetProperty("type")}");
                count++;
            }
            evType = null; data = null;
        }
        else if (line.StartsWith("event:")) evType = line[6..].Trim();
        else if (line.StartsWith("data:")) data = line[5..].Trim();
    }
}

var subTasks = new List<Task> {
    Task.Run(() => StartSubscriber("order-archive", "csharp-order-sub", 1)),
    Task.Run(() => StartSubscriber("alert-stream",  "csharp-alert-sub", 1)),
    Task.Run(() => StartSubscriber("all-events",    "csharp-all-sub",   3)),
};
await Task.Delay(500);

using var httpClient = new HttpClient();
var eventTypes = new[] {
    "com.kubemq.examples.routing.order",
    "com.kubemq.examples.routing.alert",
    "com.kubemq.examples.routing.info",
};
foreach (var evType in eventTypes)
{
    var ev = new CloudEvent {
        Id = Guid.NewGuid().ToString(),
        Type = evType,
        Source = new Uri("urn:kubemq-ce-csharp-cesql"),
        Subject = "routing-source",
        DataContentType = "application/json",
        Data = new { description = $"Event of type {evType}" },
    };
    var bytes = formatter.EncodeStructuredModeMessage(ev, out var ct);
    using var c = new ByteArrayContent(bytes.ToArray());
    c.Headers.ContentType = MediaTypeHeaderValue.Parse(ct.ToString());
    var r = await httpClient.PostAsync($"{base_}/ce/send/event", c);
    Console.WriteLine($"Published type={evType} (status={r.StatusCode})");
}

await Task.Delay(2000);
while (results.TryDequeue(out var msg)) Console.WriteLine(msg);
Console.WriteLine("\nCESQL routing demonstration complete.");

// Expected output (with CESQL routing configured):
// CESQL Routing Example — C#
// Requires KubeMQ with CESQL routing configured.
//
// Published type=com.kubemq.examples.routing.order (status=Accepted)
// Published type=com.kubemq.examples.routing.alert (status=Accepted)
// Published type=com.kubemq.examples.routing.info (status=Accepted)
//   [order-archive] type=com.kubemq.examples.routing.order
//   [alert-stream] type=com.kubemq.examples.routing.alert
//   [all-events] type=com.kubemq.examples.routing.order
//   ...
//
// CESQL routing demonstration complete.
