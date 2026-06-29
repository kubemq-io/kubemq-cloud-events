// Example: events-store/Basic
// Subscribes with StartNewOnly (events_store_type=1), publishes one event, receives it.
// Run: dotnet run
using CloudNative.CloudEvents;
using CloudNative.CloudEvents.SystemTextJson;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;

static string ServerUrl() => Environment.GetEnvironmentVariable("KUBEMQ_CE_URL") ?? "http://localhost:9090";

var base_ = ServerUrl();
var channel = "csharp-ce-events-store.basic";
var formatter = new JsonEventFormatter();
var received = new TaskCompletionSource<JsonElement>(TaskCreationOptions.RunContinuationsAsynchronously);

// Start SSE subscriber — events_store_type=1 (StartNewOnly).
var subscriberTask = Task.Run(async () =>
{
    using var http = new HttpClient { Timeout = Timeout.InfiniteTimeSpan };
    var url = $"{base_}/ce/subscribe/events-store?client_id=csharp-es-sub&channel={Uri.EscapeDataString(channel)}&events_store_type=1";
    using var req = new HttpRequestMessage(HttpMethod.Get, url);
    req.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("text/event-stream"));
    using var resp = await http.SendAsync(req, HttpCompletionOption.ResponseHeadersRead);
    using var reader = new StreamReader(await resp.Content.ReadAsStreamAsync(), Encoding.UTF8);
    string? evType = null, data = null, line;
    while ((line = await reader.ReadLineAsync()) != null)
    {
        if (line == "") {
            if (evType == "cloudevent" && data != null)
                { received.TrySetResult(JsonSerializer.Deserialize<JsonElement>(data)); return; }
            if (evType == "error") { Console.Error.WriteLine($"SSE error: {data}"); return; }
            evType = null; data = null;
        }
        else if (line.StartsWith(":")) { }
        else if (line.StartsWith("event:")) evType = line[6..].Trim();
        else if (line.StartsWith("data:")) data = line[5..].Trim();
    }
});

await Task.Delay(500);

var ev = new CloudEvent {
    Id = Guid.NewGuid().ToString(),
    Type = "com.kubemq.examples.eventsstore.stored",
    Source = new Uri("urn:kubemq-ce-csharp-example"),
    Subject = channel,
    DataContentType = "application/json",
    Data = new { msg = "hello events-store from C#!" },
};
var bytes = formatter.EncodeStructuredModeMessage(ev, out var ct);
using var content = new ByteArrayContent(bytes.ToArray());
content.Headers.ContentType = MediaTypeHeaderValue.Parse(ct.ToString());
using var httpClient = new HttpClient();
var postResp = await httpClient.PostAsync($"{base_}/ce/send/event-store", content);
Console.WriteLine($"Published to events-store: status={postResp.StatusCode}");

using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
var ce = await received.Task.WaitAsync(cts.Token);
Console.WriteLine($"Received: type={ce.GetProperty("type")} data={ce.GetProperty("data")}");

// Expected output:
// Published to events-store: status=Accepted
// Received: type=com.kubemq.examples.eventsstore.stored data={"msg":"hello events-store from C#!"}
