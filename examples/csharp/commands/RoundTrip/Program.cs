// Example: commands/RoundTrip — RPC command with execution ack.
// Run: dotnet run
using CloudNative.CloudEvents;
using CloudNative.CloudEvents.SystemTextJson;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;

static string ServerUrl() => Environment.GetEnvironmentVariable("KUBEMQ_CE_URL") ?? "http://localhost:9090";

var base_ = ServerUrl();
var channel = "csharp-ce-commands.round-trip";
var formatter = new JsonEventFormatter();

async Task RunResponder()
{
    using var http = new HttpClient { Timeout = Timeout.InfiniteTimeSpan };
    var url = $"{base_}/ce/subscribe/commands?client_id=csharp-cmd-responder&channel={Uri.EscapeDataString(channel)}";
    using var req = new HttpRequestMessage(HttpMethod.Get, url);
    req.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("text/event-stream"));
    using var resp = await http.SendAsync(req, HttpCompletionOption.ResponseHeadersRead);
    using var stream = await resp.Content.ReadAsStreamAsync();
    using var reader = new StreamReader(stream, Encoding.UTF8);
    string? evType = null, data = null, line;
    while ((line = await reader.ReadLineAsync()) != null)
    {
        if (line == "")
        {
            if (evType == "cloudevent" && data != null)
            {
                var raw = JsonSerializer.Deserialize<JsonElement>(data);
                var requestId = raw.GetProperty("_kubemq_request_id").GetString()!;
                var replyChannel = raw.GetProperty("_kubemq_reply_channel").GetString()!;
                Console.WriteLine($"[responder] command received: type={raw.GetProperty("type")} request_id={requestId}");

                var responseEvent = new CloudEvent {
                    Id = Guid.NewGuid().ToString(),
                    Type = "com.kubemq.examples.commands.response",
                    Source = new Uri("urn:csharp-cmd-responder"),
                    Subject = replyChannel,
                    DataContentType = "application/json",
                    Data = new { executed = true, status = "command processed" },
                };
                var bytes = formatter.EncodeStructuredModeMessage(responseEvent, out var ct);
                using var rc = new ByteArrayContent(bytes.ToArray());
                rc.Headers.ContentType = MediaTypeHeaderValue.Parse(ct.ToString());
                using var sendHttp = new HttpClient();
                var r = await sendHttp.PostAsync($"{base_}/ce/send/response?request_id={requestId}", rc);
                var rj = JsonSerializer.Deserialize<JsonElement>(await r.Content.ReadAsStringAsync());
                Console.WriteLine($"[responder] response sent: is_error={rj.GetProperty("is_error")}");
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

var cmdEvent = new CloudEvent {
    Id = Guid.NewGuid().ToString(),
    Type = "com.kubemq.examples.commands.reboot",
    Source = new Uri("urn:kubemq-ce-csharp-sender"),
    Subject = channel,
    DataContentType = "application/json",
    Data = new { device_id = "sensor-42", action = "reboot" },
};
var cmdBytes = formatter.EncodeStructuredModeMessage(cmdEvent, out var cmdCt);
using var cmdContent = new ByteArrayContent(cmdBytes.ToArray());
cmdContent.Headers.ContentType = MediaTypeHeaderValue.Parse(cmdCt.ToString());
using var senderHttp = new HttpClient();
Console.WriteLine("[sender] sending command...");
var cmdResp = await senderHttp.PostAsync($"{base_}/ce/send/command", cmdContent);
var cmdResult = JsonSerializer.Deserialize<JsonElement>(await cmdResp.Content.ReadAsStringAsync());
Console.WriteLine($"[sender] command ack: status={cmdResp.StatusCode} is_error={cmdResult.GetProperty("is_error")}");
await responderTask;
