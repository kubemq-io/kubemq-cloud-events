# Example: commands/round_trip — RPC command with execution ack.
require "net/http"; require "uri"; require "json"; require "timeout"; require "securerandom"; require "cloud_events"
def server_url = ENV.fetch("KUBEMQ_CE_URL", "http://localhost:9090")

base = server_url; channel = "ruby-ce-commands.round-trip"
sdk = CloudEvents::HttpBinding.default
ready = Queue.new

responder = Thread.new do
  uri = URI("#{base}/ce/subscribe/commands?client_id=ruby-cmd-responder&channel=#{URI.encode_www_form_component(channel)}")
  Net::HTTP.start(uri.host, uri.port) do |http|
    req = Net::HTTP::Get.new(uri); req["Accept"] = "text/event-stream"
    http.request(req) do |resp|
      ready.push(true) # signal that SSE connection is established
      ev_type = nil; data = nil
      resp.read_body do |chunk|
        chunk.each_line do |line|
          line.chomp!
          if line.empty?
            if ev_type == "cloudevent" && data
              raw = JSON.parse(data)
              request_id  = raw["_kubemq_request_id"]
              reply_channel = raw["_kubemq_reply_channel"]
              puts "[responder] command received: type=#{raw['type']} request_id=#{request_id}"
              resp_event = CloudEvents::Event::V1.new(
                id: SecureRandom.uuid, type: "com.kubemq.examples.commands.response",
                source: URI("urn:ruby-cmd-responder"), subject: reply_channel,
                spec_version: "1.0",
                data_content_type: CloudEvents::ContentType.new("application/json"),
                data: JSON.generate({ executed: true, status: "command processed" }))
              resp_h, resp_b = sdk.encode_event(resp_event, structured_format: "json")
              ruri = URI("#{base}/ce/send/response?request_id=#{URI.encode_www_form_component(request_id)}")
              Net::HTTP.start(ruri.host, ruri.port) do |rhttp|
                rreq = Net::HTTP::Post.new(ruri); resp_h.each{|k,v|rreq[k]=v}; rreq.body=resp_b
                rr = rhttp.request(rreq)
                puts "[responder] response sent: is_error=#{JSON.parse(rr.body)['is_error']}"
              end
              Thread.exit
            end
            ev_type = nil; data = nil
          elsif line.start_with?("event:") then ev_type = line.sub("event:","").strip
          elsif line.start_with?("data:") then data = line.sub("data:","").strip
          end
        end
      end
    end
  end
end

Timeout.timeout(5) { ready.pop }
sleep 0.1

cmd_event = CloudEvents::Event::V1.new(
  id: SecureRandom.uuid, type: "com.kubemq.examples.commands.reboot",
  source: URI("urn:kubemq-ce-ruby-sender"), subject: channel, spec_version: "1.0",
  data_content_type: CloudEvents::ContentType.new("application/json"),
  data: JSON.generate({ device_id: "sensor-42", action: "reboot" }))
cmd_h, cmd_b = sdk.encode_event(cmd_event, structured_format: "json")
uri = URI("#{base}/ce/send/command")
puts "[sender] sending command..."
Net::HTTP.start(uri.host, uri.port) do |http|
  req = Net::HTTP::Post.new(uri); cmd_h.each{|k,v|req[k]=v}; req.body=cmd_b
  res = http.request(req)
  r = JSON.parse(res.body)
  puts "[sender] command ack: status=#{res.code} is_error=#{r['is_error']}"
end
responder.join
