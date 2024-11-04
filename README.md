![Test Coverage](test/coverage.svg)

# Zencastr Message Store for Python

Message store implementation in Python

This package implements an event sourcing model of storing application data. It is similar to [Eventide](http://docs.eventide-project.org/core-concepts/event-sourcing.html#pub-sub).


## Usage

```python
# python3 -m asyncio
client = await nats.connect("nats://localhost:4222")
message_store = MessageStore(client, "env", should_create_missing_streams=True)

# Create stream
await message_store.ensure_stream("stream-name")

# Prepare subscription with handlers
subscription = message_store.create_subscription(
    "stream-name.>",
    "durable-consumer-name",
    handlers={
        "Command": lambda msg: print(msg.seq, msg.subject, msg.data, sep="\t"),
        "FailingCommand": lambda msg: 1 / 0,
    },
)
subscription.start()

# Publish messages and await processing

await message_store.publish_message("stream-name.unique-id1", Message("Command", {"key": "value"}))
# `1   stream-name.unique-id   {'key': 'value'}`
await message_store.publish_message("stream-name.unique-id2", Message("FailingCommand", {"key": "badvalue"}))
# NOTE: Default behavior is to retry 3 times, on the 4th attempt it will TERM the message
# Failed to handle message with subject env.stream-name.unique-id2, seq: 2, data: b'{"type": "FailingCommand", "data": {"key": "badvalue"}}', exception: ZeroDivisionError division by zero
# Failed to handle message with subject env.stream-name.unique-id2, seq: 2, data: b'{"type": "FailingCommand", "data": {"key": "badvalue"}}', exception: ZeroDivisionError division by zero
# Failed to handle message with subject env.stream-name.unique-id2, seq: 2, data: b'{"type": "FailingCommand", "data": {"key": "badvalue"}}', exception: ZeroDivisionError division by zero
# Giving up on processing message #2, subject env.stream-name.unique-id2 from stream env-stream-name. This attempt (#4) exceeds max
await asyncio.sleep(1)  # Leave time for the messages to be processed
```

## Authors

- Rui Figueiredo (@ruidfigueiredo)
- Alex Cannan (@alexcannan)
