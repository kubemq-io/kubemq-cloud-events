# Example: events_store/replay_from_first
# Publishes 3 events then subscribes with events_store_type=2 (StartFromFirst).
require "net/http"; require "uri"; require "json"; require "timeout"; require "cloud_events"; require "securerandom"
def server_url = ENV.fetch("KUBEMQ_CE_URL", "http://localhost:9090")

base = server_url; channel = "ruby-ce-events-store.replay-from-first"
sdk = CloudEvents::HttpBinding.default

# Publish 3 events.
puts "Publishing 3 events to events-store..."
(1..3).each do |i|
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
  puts "  Published seq=#{i}"
end

# Subscribe with StartFromFirst (events_store_type=2).
received = Queue.new
Thread.new do
  uri = URI("#{base}/ce/subscribe/events-store?client_id=ruby-es-replay&channel=#{URI.encode_www_form_component(channel)}&events_store_type=2")
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
              Thread.exit if count >= 3
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

events = []
3.times do
  raw = Timeout.timeout(10) { received.pop }
  ce = JSON.parse(raw)
  ce["data"] = JSON.parse(ce["data"]) if ce["data"].is_a?(String)
  events << ce
end
puts "Replayed #{events.size} events from first:"
events.each { |ce| puts "  seq=#{ce.dig('data','seq')} id=#{ce['id']}" }

# Expected output:
# Publishing 3 events to events-store...
#   Published seq=1
#   Published seq=2
#   Published seq=3
# Replayed 3 events from first:
#   seq=1 id=...
#   seq=2 id=...
#   seq=3 id=...
