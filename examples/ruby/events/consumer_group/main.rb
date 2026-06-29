# Example: events/consumer_group — two subscribers in same group, each gets one event.
require "net/http"
require "uri"
require "json"
require "timeout"
require "securerandom"
require "cloud_events"

def server_url
  ENV.fetch("KUBEMQ_CE_URL", "http://localhost:9090")
end

base    = server_url
channel = "ruby-ce-events.consumer-group"
group   = "workers"
results = Queue.new

def subscribe_group(base, channel, group, client_id, results)
  Thread.new do
    uri = URI("#{base}/ce/subscribe/events?client_id=#{client_id}&channel=#{URI.encode_www_form_component(channel)}&group=#{group}")
    Net::HTTP.start(uri.host, uri.port) do |http|
      req = Net::HTTP::Get.new(uri)
      req["Accept"] = "text/event-stream"
      http.request(req) do |resp|
        ev_type = nil; data = nil
        resp.read_body do |chunk|
          chunk.each_line do |line|
            line.chomp!
            if line.empty?
              if ev_type == "cloudevent" && data
                results.push("[#{client_id}] received: #{data[0, 80]}")
                Thread.exit
              end
              ev_type = nil; data = nil
            elsif line.start_with?("event:") then ev_type = line.sub("event:", "").strip
            elsif line.start_with?("data:") then data = line.sub("data:", "").strip
            end
          end
        end
      end
    end
  end
end

subscribe_group(base, channel, group, "ruby-worker-1", results)
subscribe_group(base, channel, group, "ruby-worker-2", results)
sleep 0.6

sdk = CloudEvents::HttpBinding.default
(1..2).each do |seq|
  event = CloudEvents::Event::V1.new(
    id:           SecureRandom.uuid, type: "com.kubemq.examples.events.grouped",
    source:       URI("urn:kubemq-ce-ruby-example"), subject: channel,
    spec_version: "1.0",
    data_content_type: CloudEvents::ContentType.new("application/json"),
    data: JSON.generate({ seq: seq })
  )
  enc_headers, enc_body = sdk.encode_event(event, structured_format: "json")
  uri = URI("#{base}/ce/send/event")
  Net::HTTP.start(uri.host, uri.port) do |http|
    req = Net::HTTP::Post.new(uri)
    req["Content-Type"] = enc_headers["Content-Type"]
    req.body = enc_body
    res = http.request(req)
    puts "Published event seq=#{seq} (status=#{res.code})"
  end
end

2.times { puts Timeout.timeout(10) { results.pop } }
