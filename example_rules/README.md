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
   "event_type": "post",
   "post": {
      "text": "here is some text"
   }
}
```

To moderate these events with Osprey, you can produce them on a Kafka event bus in the following format which Osprey understands:

```json
{ "send_time": "2025-08-25T14:30:45.123456789Z", "data": "{\"action_id\": 1, \"action_name\": \"create_post\", \"data\":{\"user_id\": \"user_1923\", \"event_type\": \"create_post\", \"post\": { \"text\": \"hello world\" }}}"}
```

## Try it out

From inside the Osprey project root directory, run `docker compose up`. This will start an Osprey worker and a Kafka server.

Next, use `kafka-console-producer` to send a test event to the Osprey Kafka consumer:

```sh
# Note that on some Linux distrubtions, this may be `kafka-console-producer.sh` rather than `kafka-console-producer`.
kafka-console-producer --bootstrap-server localhost:9092 --topic osprey.actions_input
```

Copy and paste the event above into the producer, then check the Osprey worker's logs. You should see an event that has a `ban-user` effect attached to it. The output JSON looks like this:

```json
{
  "__action_id": 1,
  "__timestamp": "2025-08-25T14:30:45.123456+00:00",
  "__error_count": 0,
  "__ban_user": [
    "user_1923|User said \"hello\""
  ],
  "EventType": "create_post",
  "PostText": "hello world",
  "UserId": "user_1923",
  "ContainsHello": true
}
```

You can also try sending an event that does not contain the word "hello", and you should see that no effects are added to the event.

```json
{
  "__action_id": 1,
  "__timestamp": "2025-08-25T14:30:45.123456+00:00",
  "__error_count": 0,
  "EventType": "create_post",
  "PostText": "i wont say that word",
  "UserId": "user_1923",
  "ContainsHello": false
}
```
