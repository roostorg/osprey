# Osprey Example Rules

This example rules project demonstrates how to write a simple rule that:

- Creates entities based on items inside of your event JSON
- Uses a UDF to return a `true`/`false` value, which determines whether a user should be banned or not
- Adds an "effect" to the event using a UDF

For this example, imagine that you are attempting to moderate a social media network. The one rule on your networks is that
users may never say "hello" to each other. As a result, you want to ban any user who ever mentions the word "hello".

Posts on your network are a JSON object that looks like this:

```json
{
   "text": "here is some text"
}
```

Your users also have user IDs which are strings. Whenever a user creates an event, imagine that you normally propagate those events like so:

```json
{
   "user_id": "user_1923",
   "event_type": "create_post",
   "post": {
      "text": "here is some text"
   }
}
```

To moderate these events with Osprey, you can produce them on a Kafka event bus in the following format which Osprey understands:

```json
{"send_time": "2025-08-25T14:30:45.123Z", "data": {"action_id": 1, "action_name": "create_post", "data": {"user_id": "user_1923", "event_type": "create_post", "post": {"text": "hello world"}}}}
```

`data` is a JSON object, not a string containing JSON; the worker can't read a string there and drops the event.

## Try it out

From inside the Osprey project root directory, run `docker compose up -d`. This starts the whole Osprey stack, including Kafka, the Osprey worker, Postgres, Druid, and the Osprey UI and UI API.

Next, use `kafka-console-producer` to send a test event to the Osprey Kafka consumer:

```sh
docker compose exec osprey-kafka kafka-console-producer --bootstrap-server localhost:9092 --topic osprey.actions_input
```

(If you have the Kafka tools installed on your machine, `kafka-console-producer` works directly; on some Linux distributions it's `kafka-console-producer.sh`.)

Copy and paste the event above into the producer, then check the Osprey worker's logs (`docker compose logs osprey-worker`). You should see a result with a `__ban_user` effect and a `meow` label mutation attached to it. The output JSON looks like this:

```json
{
  "__action_id": 1,
  "__timestamp": "2025-08-25T14:30:45.123000+00:00",
  "__error_count": 0,
  "__ban_user": [
    "user_1923|User said \"hello\""
  ],
  "__entity_label_mutations": [
    "User/meow/1"
  ],
  "ActionName": "create_post",
  "ActionId": 1,
  "UserId": "user_1923",
  "EventType": "create_post",
  "PostText": "hello world",
  "ContainsHello": true
}
```

You can also try sending an event that does not contain the word "hello", and you should see that no effects are added to the event.

```json
{
  "__action_id": 1,
  "__timestamp": "2025-08-25T14:30:45.123000+00:00",
  "__error_count": 0,
  "ActionName": "create_post",
  "ActionId": 1,
  "UserId": "user_1923",
  "EventType": "create_post",
  "PostText": "i wont say that word",
  "ContainsHello": false
}
```
