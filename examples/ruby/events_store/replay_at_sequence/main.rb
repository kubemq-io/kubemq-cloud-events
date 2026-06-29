# Example: events_store/replay_at_sequence
# Publishes 5 events then subscribes with events_store_type=4 + events_store_value=3.
require "net/http"; require "uri"; require "json"; require "timeout"; require "cloud_events"; require "securerandom"
def server_url = ENV.fetch("KUBEMQ_CE_URL", "http://localhost:9090")

base = server_url; channel = "ruby-ce-events-store.replay-at-sequence"
sdk = CloudEvents::HttpBinding.default; start_at_seq = 3

puts "Publishing 5 events to events-store..."
(1..5).each do |i|
  ev = CloudEvents::Event::V1.new(
    id: SecureRandom.uuid, type: "com.kubemq.examples.eventsstore.stored",
    source: URI("urn:kubemq-ce-ruby-example"), subject: channel, spec_version: "1.0",
    data_content_type: CloudEvents::ContentType.new("application/json"),
    data: JSON.generate({ seq: i })
  )
  enc_h, enc_b = sdk.encode_event(ev, structured_format: "json")
  uri = URI("#{base}/ce/send/event-store")
  Net::HTTP.start(uri.host, uri.port) do |http|
    req = Net::HTTP::Post.new(uri); enc_h.each{|k,v|req[k]=v}; req.body=enc_b
    http.request(req)
  end
end
puts "Published 5 events. Subscribing from sequence #{start_at_seq}..."

# events_store_type=4 (StartAtSequence), events_store_value=start_at_seq
received = Queue.new
expected_count = 5 - start_at_seq + 1
Thread.new do
  uri = URI("#{base}/ce/subscribe/events-store?client_id=ruby-es-seq&channel=#{URI.encode_www_form_component(channel)}&events_store_type=4&events_store_value=#{start_at_seq}")
  Net::HTTP.start(uri.host, uri.port) do |http|
    req = Net::HTTP::Get.new(uri); req["Accept"] = "text/event-stream"
    http.request(req) do |resp|
      ev_type = nil; data = nil; count = 0
      resp.read_body do |chunk|
        chunk.each_line do |line|
          line.chomp!
          if line.empty?
            if ev_type == "cloudevent" && data
              received.push(data); count += 1
              Thread.exit if count >= expected_count
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

puts "Expecting #{expected_count} events from sequence #{start_at_seq}:"
expected_count.times do
  raw = Timeout.timeout(10) { received.pop }
  ce = JSON.parse(raw)
  ce["data"] = JSON.parse(ce["data"]) if ce["data"].is_a?(String)
  puts "  Received: seq=#{ce.dig('data','seq')}"
end

# Expected output:
# Publishing 5 events to events-store...
# Published 5 events. Subscribing from sequence 3...
# Expecting 3 events from sequence 3:
#   Received: seq=3
#   Received: seq=4
#   Received: seq=5
