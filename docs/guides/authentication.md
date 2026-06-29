# Authentication

## Overview

The KubeMQ CE connector uses the same authentication middleware as all other connectors on the shared HTTP server (port 9090). Authentication is JWT/OIDC-based and configured in the KubeMQ server configuration.

For detailed auth server configuration, see the KubeMQ server authentication documentation (`07-authentication-authorization.md`).

## ClientID Override

When authentication is enabled and a request carries valid credentials, the authenticated `ClientID` from the JWT claims **overrides** the CE `source` attribute:

```
CE source attribute → ClientID (default, no auth)
JWT/OIDC claims ClientID → ClientID (when auth enabled, overrides source)
```

## Passing Auth Token in CE HTTP Requests

Include the bearer token in the `Authorization` header of your HTTP request:

```bash
curl -X POST http://localhost:9090/ce/send/event \
  -H "Authorization: Bearer your-jwt-token" \
  -H "Content-Type: application/cloudevents+json" \
  -d '{
    "specversion": "1.0",
    "type": "com.example.event",
    "source": "my-service",
    "subject": "my-channel",
    "data": {}
  }'
```

For SSE subscriptions:

```bash
curl -N "http://localhost:9090/ce/subscribe/events?client_id=my-client&channel=my-channel" \
  -H "Authorization: Bearer your-jwt-token"
```

## No-Auth Configuration

In development or trusted network environments, authentication can be disabled. The CE `source` attribute is then used directly as the `ClientID`. All examples in this repository assume no authentication is configured.

## See Also

- KubeMQ server auth docs: `07-authentication-authorization.md`
