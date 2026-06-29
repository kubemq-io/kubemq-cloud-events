# Example: queries/round_trip — RPC query with data response.
require "net/http"; require "uri"; require "json"; require "timeout"; require "securerandom"; require "cloud_events"
def server_url = ENV.fetch("KUBEMQ_CE_URL", "http://localhost:9090")

base = server_url; channel = "ruby-ce-queries.round-trip"
sdk = CloudEvents::HttpBinding.default
inventory = { "WIDGET-100" => 42, "GADGET-200" => 7 }
ready = Queue.new

responder = Thread.new do
  uri = URI("#{base}/ce/subscribe/queries?client_id=ruby-query-responder&channel=#{URI.encode_www_form_component(channel)}")
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
              request_id = raw["_kubemq_request_id"]
              reply_channel = raw["_kubemq_reply_channel"]
              raw_data = raw["data"]
              raw_data = JSON.parse(raw_data) if raw_data.is_a?(String)
              sku = raw_data.is_a?(Hash) ? raw_data["sku"] : nil
              qty = inventory[sku] || 0
              puts "[responder] query sku=#{sku} qty=#{qty} request_id=#{request_id}"
              resp_ev = CloudEvents::Event::V1.new(
                id: SecureRandom.uuid,
                type: "com.kubemq.examples.queries.inventory-result",
                source: URI("urn:ruby-query-responder"), subject: reply_channel,
                spec_version: "1.0",
                data_content_type: CloudEvents::ContentType.new("application/json"),
                data: JSON.generate({ sku: sku, quantity: qty }))
              resp_h, resp_b = sdk.encode_event(resp_ev, structured_format: "json")
              ruri = URI("#{base}/ce/send/response?request_id=#{URI.encode_www_form_component(request_id)}")
              Net::HTTP.start(ruri.host, ruri.port) do |rhttp|
                rreq = Net::HTTP::Post.new(ruri); resp_h.each{|k,v|rreq[k]=v}; rreq.body=resp_b
                rhttp.request(rreq)
              end
              puts "[responder] response sent."
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

Timeout.timeout(5) { ready.pop }; sleep 0.1

ev = CloudEvents::Event::V1.new(
  id: SecureRandom.uuid, type: "com.kubemq.examples.queries.inventory-check",
  source: URI("urn:kubemq-ce-ruby-sender"), subject: channel, spec_version: "1.0",
  data_content_type: CloudEvents::ContentType.new("application/json"),
  data: JSON.generate({ sku: "WIDGET-100" }))
ev_h, ev_b = sdk.encode_event(ev, structured_format: "json")
uri = URI("#{base}/ce/send/query")
puts "[sender] sending query for sku=WIDGET-100..."
Net::HTTP.start(uri.host, uri.port) do |http|
  req = Net::HTTP::Post.new(uri); ev_h.each{|k,v|req[k]=v}; req.body=ev_b
  res = http.request(req)
  r = JSON.parse(res.body)
  puts "[sender] query response: status=#{res.code} is_error=#{r['is_error']}"
  puts "[sender] response data: #{r['data']}"
end
responder.join
