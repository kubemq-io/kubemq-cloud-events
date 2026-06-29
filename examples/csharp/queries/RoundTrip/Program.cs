// Example: queries/RoundTrip — RPC query with data response.
// Run: dotnet run
using CloudNative.CloudEvents;
using CloudNative.CloudEvents.SystemTextJson;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;

static string ServerUrl() => Environment.GetEnvironmentVariable("KUBEMQ_CE_URL") ?? "http://localhost:9090";

var base_ = ServerUrl();
var channel = "csharp-ce-queries.round-trip";
var formatter = new JsonEventFormatter();
var inventory = new Dictionary<string, int> { ["WIDGET-100"] = 42, ["GADGET-200"] = 7 };
var responderDone = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

async Task RunResponder()
{
    using var http = new HttpClient { Timeout = Timeout.InfiniteTimeSpan };
    var url = $"{base_}/ce/subscribe/queries?client_id=csharp-query-responder&channel={Uri.EscapeDataString(channel)}";
    using var req = new HttpRequestMessage(HttpMethod.Get, url);
    req.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("text/event-stream"));
    using var resp = await http.SendAsync(req, HttpCompletionOption.ResponseHeadersRead);
    using var reader = new StreamReader(await resp.Content.ReadAsStreamAsync(), Encoding.UTF8);
    string? evType = null, data = null, line;
    while ((line = await reader.ReadLineAsync()) != null)
    {
        if (line == "") {
            if (evType == "cloudevent" && data != null) {
                var raw = JsonSerializer.Deserialize<JsonElement>(data);
                var requestId = raw.GetProperty("_kubemq_request_id").GetString()!;
                var replyChannel = raw.GetProperty("_kubemq_reply_channel").GetString()!;
                var sku = raw.GetProperty("data").GetProperty("sku").GetString()!;
                var qty = inventory.GetValueOrDefault(sku, 0);
                Console.WriteLine($"[responder] query sku={sku} qty={qty} request_id={requestId}");

                var respEvent = new CloudEvent {
                    Id = Guid.NewGuid().ToString(),
                    Type = "com.kubemq.examples.queries.inventory-result",
                    Source = new Uri("urn:csharp-query-responder"),
                    Subject = replyChannel,
                    DataContentType = "application/json",
                    Data = new { sku, quantity = qty },
                };
                var bytes = formatter.EncodeStructuredModeMessage(respEvent, out var ct);
                using var rc = new ByteArrayContent(bytes.ToArray());
                rc.Headers.ContentType = MediaTypeHeaderValue.Parse(ct.ToString());
                using var sendHttp = new HttpClient();
                await sendHttp.PostAsync($"{base_}/ce/send/response?request_id={requestId}", rc);
                Console.WriteLine("[responder] response sent.");
                responderDone.TrySetResult();
                return;
            }
            evType = null; data = null;
        }
        else if (line.StartsWith("event:")) evType = line[6..].Trim();
        else if (line.StartsWith("data:")) data = line[5..].Trim();
    }
}

var responderTask = RunResponder();
await Task.Delay(500);

// Send query.
var query = new CloudEvent {
    Id = Guid.NewGuid().ToString(),
    Type = "com.kubemq.examples.queries.inventory-check",
    Source = new Uri("urn:kubemq-ce-csharp-sender"),
    Subject = channel,
    DataContentType = "application/json",
    Data = new { sku = "WIDGET-100" },
};
var qBytes = formatter.EncodeStructuredModeMessage(query, out var qCt);
using var qContent = new ByteArrayContent(qBytes.ToArray());
qContent.Headers.ContentType = MediaTypeHeaderValue.Parse(qCt.ToString());
using var senderHttp = new HttpClient();
Console.WriteLine("[sender] sending query for sku=WIDGET-100...");
var queryResp = await senderHttp.PostAsync($"{base_}/ce/send/query", qContent);
var queryJ = JsonSerializer.Deserialize<JsonElement>(await queryResp.Content.ReadAsStringAsync());
Console.WriteLine($"[sender] query response: status={queryResp.StatusCode} is_error={queryJ.GetProperty("is_error")}");
Console.WriteLine($"[sender] response data: {queryJ.GetProperty("data")}");
await responderDone.Task;

// Expected output:
// [responder] query sku=WIDGET-100 qty=42 request_id=...
// [responder] response sent.
// [sender] sending query for sku=WIDGET-100...
// [sender] query response: status=OK is_error=False
// [sender] response data: {"sku":"WIDGET-100","quantity":42}
