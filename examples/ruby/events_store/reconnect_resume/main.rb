# Example: events_store/reconnect_resume
# Demonstrates Last-Event-ID reconnect. Receives 2 events recording SSE id,
# then reconnects with Last-Event-ID header (no events_store_type param).
require "net/http"; require "uri"; require "json"; require "cloud_events"; require "securerandom"
def server_url = ENV.fetch("KUBEMQ_CE_URL", "http://localhost:9090")

base = server_url; channel = "ruby-ce-events-store.reconnect-resume"
sdk = CloudEvents::HttpBinding.default

# Publish 4 events.
puts "Publishing 4 events..."
(1..4).each do |i|
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

# Helper: open one SSE connection, collect up to max_events, return [events, last_id].
def subscribe_es(base, channel, last_event_id, max_events)
  # Omit events_store_type when reconnecting with Last-Event-ID.
  query = last_event_id.nil? \
    ? "events_store_type=2" \
    : ""  # no events_store_type on reconnect
  uri = URI("#{base}/ce/subscribe/events-store?client_id=ruby-es-reconnect&channel=#{URI.encode_www_form_component(channel)}#{query.empty? ? '' : '&' + query}")
  events = []; last_id = nil
  Net::HTTP.start(uri.host, uri.port, read_timeout: 12) do |http|
    req = Net::HTTP::Get.new(uri); req["Accept"] = "text/event-stream"
    req["Last-Event-ID"] = last_event_id if last_event_id
    http.request(req) do |resp|
      ev_type = nil; data = nil; id = nil
      resp.read_body do |chunk|
        chunk.each_line do |line|
          line.chomp!
          if line.empty?
            if ev_type == "cloudevent" && data
              last_id = id if id
              ce = JSON.parse(data)
              ce["data"] = JSON.parse(ce["data"]) if ce["data"].is_a?(String)
              events << ce
              return [events, last_id] if events.size >= max_events
            end
            ev_type = nil; data = nil; id = nil
          elsif line.start_with?("id:") then id = line.sub("id:","").strip
          elsif line.start_with?("event:") then ev_type = line.sub("event:","").strip
          elsif line.start_with?("data:") then data = line.sub("data:","").strip
          end
        end
      end
    end
  end
  [events, last_id]
end

# First connection: receive 2 events.
puts "First connection — receiving 2 events:"
first_events, last_id = subscribe_es(base, channel, nil, 2)
first_events.each { |ce| puts "  Received: seq=#{ce.dig('data','seq')}" }
puts "  Last-Event-ID recorded: #{last_id}"

# Reconnect using Last-Event-ID.
puts "Reconnecting with Last-Event-ID=#{last_id}..."
second_events, _ = subscribe_es(base, channel, last_id, 2)
second_events.each { |ce| puts "  Resumed: seq=#{ce.dig('data','seq')}" }
puts "Reconnect-resume demonstration complete."

# Expected output:
# Publishing 4 events...
# First connection — receiving 2 events:
#   Received: seq=1
#   Received: seq=2
#   Last-Event-ID recorded: 2
# Reconnecting with Last-Event-ID=2...
#   Resumed: seq=3
#   Resumed: seq=4
# Reconnect-resume demonstration complete.
