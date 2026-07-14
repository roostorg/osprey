# Concepts

Osprey watches a stream of things happening on your platform, runs each one through your rules in real time, and records what it found so you can query and act on it. This page defines the terms the rest of these docs (and the UI itself) use. They're listed in the order data flows through Osprey, and later concepts build on earlier ones, so it's worth a read top to bottom.

Throughout, imagine you're moderating a small social network where users create posts—the same scenario Osprey's example rules and [demo](../development/) use.

## Actions and events

Anything that happens on your platform—someone creates a post, sends a message, registers an account—can be sent to Osprey as it happens. Your platform likely calls these **events**; once inside Osprey, each one is called an **action**. The two words name the same thing, and you'll meet both: the query bar filters on `ActionName`, while the UI's live feed is the Event Stream. These docs say "event" when talking about your platform and "action" when talking about Osprey's view of one.

An action arrives with a name, like `create_post`, and whatever JSON data your platform sent along—say, the author's user ID and the post text "hello world."

## Features

A **feature** is a named value that your rules extract from each action: `PostText`, `UserId`, `AccountAgeSeconds`. Features are what you query on—every feature your rules define is queryable by name, unless its name starts with an underscore, which keeps it private to the rule file that defined it.

For the "hello world" post, your rules might extract `UserId`, `PostText`, and `EventType` as features.

## Entities

An **entity** is a feature that identifies a _thing_ on your platform rather than describing one—a user ID, an email address, an IP address. Entities are declared with a type (like `User`) so Osprey knows which values refer to the same thing across many actions. That identity is what makes labels (below) possible, and it's why clicking an entity in the UI opens a view of everything Osprey knows about it.

In our example, `UserId` would be declared as an entity: the same user posts many times, and you want Osprey to remember them.

## Rules

A **rule** is a named condition over features, written in Osprey's rule language, SML: "the event is a post creation and the post text contains 'hello'." Rules are evaluated against every action as it arrives. On their own rules just produce a true or false—which is itself queryable, like any feature—but they can be wired to effects.

Rules live in files your team reviews and versions like any other code; [Writing Rules](../rules.md) covers the language.

## Effects

An **effect** is what a matched rule does beyond evaluating to true: ban the user, add a label to an entity, declare a verdict. Effects are recorded on the action's result, so an investigator can always see afterward exactly what Osprey did and why.

When the demo's `ContainsHello` rule matches our "hello world" post, it fires two effects: a ban for the author, and a `meow` label added to them.

## Labels

A **label** is a tag on an entity that persists across actions—Osprey's memory. Rules add and remove labels as an effect, and can also _check_ labels, so past decisions inform future ones: "flag this post if its author was previously labeled a spammer." You can also add or remove labels by hand from the UI, one entity at a time or in bulk.

Labels have a name, the entity types they apply to, and a connotation (positive or negative). [Labels](investigate/labels.md) covers how they behave in investigations.

## Verdicts

A **verdict** is Osprey's answer when a caller is waiting for one. Most deployments feed Osprey from a queue and read results the same way, but a service can also submit an action synchronously and get a response; rules declare verdict strings, and those—along with any labels applied—are what the caller gets back. If your deployment only consumes results asynchronously, you may never handle a verdict directly, though you can spot them among an action's recorded features in the UI.

## Results, and where they surface

Every processed action produces an execution result: the extracted features, the rules that matched, and the effects that fired. Results are indexed for querying—that's what the whole [Investigate](investigate/) side of the UI reads. The query bar filters results by feature (`ContainsHello == True`), charts aggregate them over time, Top N groups them by any feature, and the Event Stream shows them one by one as they happen. Clicking any entity opens its details: current labels and its history on your platform.

How results get from the engine to those views (and into your own systems) is plumbing your developers control; the [Data Flow](../data-flow.md) page covers it.

## Where to go from here

Try these ideas out on live sample data by running the [demo](../development/), then head to [Investigate](investigate/) to learn the query workflow, or [Writing Rules](../rules.md) when you're ready to automate a decision of your own.
