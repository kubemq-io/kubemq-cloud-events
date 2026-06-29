// Example: events/ConsumerGroup — two subscribers in same group, each gets one event.
// Run: dotnet run
using CloudNative.CloudEvents;
using CloudNative.CloudEvents.SystemTextJson;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;

static string ServerUrl() => Environment.GetEnvironmentVariable("KUBEMQ_CE_URL") ?? "http://localhost:9090";

var base_ = ServerUrl();
var channel = "csharp-ce-events.consumer-group";
var group = "workers";

async Task<JsonElement> SubscribeGroup(string clientId)
{
    using var http = new HttpClient { Timeout = Timeout.InfiniteTimeSpan };
    var url = $"{base_}/ce/subscribe/events?client_id={clientId}&channel={Uri.EscapeDataString(channel)}&group={group}";
    using var req = new HttpRequestMessage(HttpMethod.Get, url);
    req.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("text/event-stream"));
    using var resp = await http.SendAsync(req, HttpCompletionOption.ResponseHeadersRead);
    using var stream = await resp.Content.ReadAsStreamAsync();
    using var reader = new StreamReader(stream, Encoding.UTF8);
    string? evType = null, data = null, line;
    while ((line = await reader.ReadLineAsync()) != null)
    {
        if (line == "") {
            if (evType == "cloudevent" && data != null)
                return JsonSerializer.Deserialize<JsonElement>(data);
            evType = null; data = null;
        }
        else if (line.StartsWith("event:")) evType = line[6..].Trim();
        else if (line.StartsWith("data:")) data = line[5..].Trim();
    }
    throw new Exception("SSE ended");
}

var t1 = SubscribeGroup("csharp-worker-1");
var t2 = SubscribeGroup("csharp-worker-2");
await Task.Delay(600);

var formatter = new JsonEventFormatter();
using var httpClient = new HttpClient();
for (int seq = 1; seq <= 2; seq++)
{
    var ev = new CloudEvent {
        Id = Guid.NewGuid().ToString(),
        Type = "com.kubemq.examples.events.grouped",
        Source = new Uri("urn:kubemq-ce-csharp-example"),
        Subject = channel,
        DataContentType = "application/json",
        Data = new { seq },
    };
    var bytes = formatter.EncodeStructuredModeMessage(ev, out var ct);
    using var content = new ByteArrayContent(bytes.ToArray());
    content.Headers.ContentType = MediaTypeHeaderValue.Parse(ct.ToString());
    var r = await httpClient.PostAsync($"{base_}/ce/send/event", content);
    Console.WriteLine($"Published event seq={seq} (status={r.StatusCode})");
}

var results = await Task.WhenAll(t1, t2);
Console.WriteLine($"[worker-1] type={results[0].GetProperty("type")} data={results[0].GetProperty("data")}");
Console.WriteLine($"[worker-2] type={results[1].GetProperty("type")} data={results[1].GetProperty("data")}");
