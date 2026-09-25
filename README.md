# go-telemetry-pipeline

Go HTTP API for validating metric events. Kafka and PostgreSQL integration is planned.

# Running the application

Start the server with go run ./cmd/telemetry.

Check that the server is running by sending a POST request to /metrics on port 8080.

Example:
```
curl -v http://127.0.0.1:8080/metrics -H "Content-Type: application/json" -d '{"timestamp": 0, "key": "input_traffic", "value": 0}'
```

The route expects to receive a JSON in request's body representing an event with timestamp, key and value.

Timestamp is an integer representing a unix epoch time in seconds. Key is a string to identify the name of the property and value is a double precision representing the property value.

The fields timestamp and value are required and accept value 0. The field key is required and does not accept empty string.

A valid event responds with HTTP status 200 and invalid json or valid json with invalid fields with status 400.

GET requests are not allowed and response for this request is status 405.

Currently the handler only evaluates the event. No action is dispatched to Kafka or any other service.
