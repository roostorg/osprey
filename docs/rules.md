# Osprey Docs

# Osprey Docs

![images/rules_architecture.png](images/rules_architecture.png)

## Rules

### Creating Rules

Rules in Osprey are written in `Some Madeup Language (SML)` and follow most syntax conventions present in the Osprey Query UI. SML is a subset of Python with additional restrictions to make the rules simpler to craft.

Rules by themselves only create variables, and without a corresponding `WhenRules()` function call, the rule will have no effects outside of evaluation and query functionality.

Rules at present support the following concepts through the `Rule()` function of the same name.

- Name

    `Rule_Name = Rule()`

    The name of the rule also functions as a conventional “RuleId” and the name of the bool that can be used to query individual rule hits in the Osprey Query UI. As a result, changing the name of a rule after activation may affect historical query results in the UI if not logged externally.

- Logic

    `when_all=[]`

    The actual logic that will be used to evaluate Osprey rules is all encompassed as single comma-delimited list of signals within the `when_all` parameter of the Rule() function and supports the use of Labels, Plugins, UDFs and other values to help enrich heuristics.

    At present, when evaluating UDFs or abstracted variables, any `NULL` evaluations in the series will cause the entire rule function to evaluate as `NULL`, which may be undesirable.

- Description

    `description=f''`

    There is an additional string description field that is able to be emitted alongside the rule itself to external systems such as logging and ticketing systems to help enrich work-streams that may benefit from plain-language context on what the rule criteria is and what the rule may intend to do.

    It may be helpful to include dynamic variables as well to help enrich operational workflows that may need to identify specific values related to the trigger criteria.


An example is below of a simple rule using various signal evaluations and out-of-the-box UDFs.

```python
My_Rule_Name_v2 = Rule(
    when_all=[
        # Primary Signal
        MyFirstValue == True,
        HasLabel(entity=MyEntityName, label='MyLabel'),
        ListLength(list=UsersValues) == 5,
        # Secondary Signal
        RegexMatch(target=MyStringValue, pattern='(hello|world)'),
        MySecondValue >= 3,
        MyThirdValue != Null,
        # Guardrail Signal
        (_LocalValue in [1, 2, 3, 5]) or (GlobalValue in ['hello', 'howdy']),
        not HasLabel(entity=MySecondEntityName, label='MySecondLabel'),
    ],
    description=f"{UserA} performed {ActionB} in this way. Emit warning",
)
```

### Instrumenting Rules with WhenRules

The `WhenRules()` function allows for the connection of rules with external services, declarations or internal label modifications by listing Rule objects in sequence within the `rules_any=[]` parameter and `EffectBase`. By default, operators and designers can utilize UDFs with predefined effects such as `DeclareVerdict()`, `LabelAdd()`, and `LabelRemote()` on positive rule evaluations.

Below is an example of the use of a WhenRules() block to verify and email and reject a request.

```python
WhenRules(
    rules_any=[
        Enabled_Rule_1,
        Enabled_Rule_2,
        # Staged_Rule_1,
    ],
    then=[
        # Verdicts
        DeclareVerdict(verdict='reject'),
        # Labels
        LabelAdd(entity=UserId, label='recently_challenged', expires_after=TimeDelta(days=7)),
        LabelAdd(entity=UserId, label='verify', apply_if=NotVerified),
        LabelAdd(entity=Email, label='pending_verify'),
        LabelAdd(entity=Domain, label='recently_seen', expires_after=TimeDelta(days=7)),
    ],
)
```

WhenRules() must occur after rule creations within the file, and may become difficult to interpret outcomes of rules if too distributed so it can be beneficial to place any effects toward the bottom of workflows.

## Output Sinks

After the rules are all run, a set of output sinks takes the resulting `ExecutionOutput` and performs additional work based on that data. These may be defined as part of a plugin as a means to perform domain specific work.

Some default use cases include a `StdoutOutputSink` which simply outputs the result to the log, a `KafkaOutputSink` which pipes data to Kafka (used for Osprey UI), or the `LabelsSink` which can add some stateful data to be used in future rules executions.

```python
class StdoutOutputSink(BaseOutputSink):
    """An output sink that prints to standard out!"""

    def __init__(self, log_sampler: Optional[DynamicLogSampler] = None):
        pass

    def will_do_work(self, result: ExecutionResult) -> bool:
        return True

    def push(self, result: ExecutionResult) -> None:
        print(f'result: {result.extracted_features_json} {result.verdicts}')

    def stop(self) -> None:
        pass
```

Passing data to these output sinks is standardized through the use of `Effects`, which are outputs of some functions, usually UDFs.

```python
def push(self, result: ExecutionResult) -> None:
    users_to_ban = result.effects[BanUserEffect]
    ban_users(users_to_ban)
```

## User Defined Functions (UDFs)

User Defined Functions (UDFs) are plugins that enable users of Osprey to extend and customize their use of the Osprey SML. UDFs are implemented python functions defined and registered as a plugin. They extend the `UDFBase` abstract base class with a set of arguments, and an output. These will be executed whenever called in the sml.

```python
# example_plugins/text_[contains.py](http://contains.py)
class TextContainsArguments(ArgumentsBase):
    text: str
    phrase: str
    case_sensitive = False

class TextContains(UDFBase[TextContainsArguments, bool]):
    def execute(self, execution_context: ExecutionContext, arguments: TextContainsArguments) -> bool:
        escaped = re.escape(arguments.phrase)
        pattern = rf'\b{escaped}\b'
        flags = 0 if [arguments.case](http://arguments.case)_sensitive else re.IGNORECASE
        regex = re.compile(pattern, flags)
        return bool([regex.search](http://regex.search)(arguments.text))

# example_plugins/register_[plugins.py](http://plugins.py)
@hookimpl_osprey
def register_udfs():
    return [TextContains]
```

Usage in SML:

```python
# example_rules/post_contains_hello.sml
ContainsHello = Rule(
  when_all=[
    EventType == 'create_post',
    TextContains(text=PostText, phrase='hello'),
  ],
  description='Post contains the word "hello"',
)
```

### Effects

Plugins may also define external effects, which are useful for performing functionality in your primary service. Effects are simply passed to output sinks at the end of a rule run. These UDFs have an output that extends `EffectBase`, and can be called as a result of a `WhenRules`.

```python
# example_plugins/src/ban_[user.py](http://user.py)
class BanUser(UDFBase[BanUserArguments, BanUserEffect]):
    category = UdfCategories.ENGINE

    def execute(self, execution_context: ExecutionContext, arguments: BanUserArguments) -> BanUserEffect:
        return BanUserEffect(
            entity=arguments.entity,
            comment=arguments.comment,
        )

# example_rules/post_contains_hello.sml
WhenRules(
  rules_any=[ContainsHello],
  then=[BanUser(entity=UserId, comment='User said "hello"')],
)
```

UDF outputs can also implement the `CustomExtractedFeature` interface - which get persisted in the outputs for the UI. `EffectToCustomExtractedFeatureBase` can also be used when effects need additional processing for use in the UI.

## Labels

Labels are a standard plugin that enable stateful rules, and touch many parts of Osprey. They are effectively tags on various entities, which can be arbitrarily defined.

### Creating Entities

Labels are applied to Entities, which are dynamically interpreted from outputs of the UDF `EntityJson`, usually applied to pieces of data that are generally consistent across actions such as User ID or email.

```python
# user.sml
UserId: Entity[str] = EntityJson(
  type='User',
  path='$.user_id'
)
```

It is possible to create new UDFs that also create entities by having the output of UDF set to `EntityT`.

### Adding Labels

Labels can be added in `WhenRules` clause. This will cause the labels output sink to tag the given entity with the given label at the end of the rules run.

```python
WhenRules(
    rules_any=[
        Sent_Too_Many_DMs,
    ],
    then=[
        LabelAdd(entity=UserId, label='likely_spammer')
    ],
)
```

### Using Labels

Since Labels can be retrieved during a rule run, they can be effectively used as state for your rules.

```python
Should_Warn_User_Of_Spammer = Rule(
    when_all=[
        HasLabel(entity=UserId, label='likely_spammer'),
        This_Is_A_New_DM,
    ],
)
```

Labels will also be shown in the UI for entities, and can also be set manually. Note that since the UI only searches across actions, `HasLabel` will not work in the Query UI. Instead, you may use `DidAddLabel`, which will be true when the given action added a label to a specific entity.

```python
# UI Query
DidAddLabel(entity_type="UserId", label_name="likely_spammer")
```

## Notable Gotchas

### Nulls

Nulls are the case where a rule or variable in SML does not exist. This can occur for many reasons - either a piece of data is missing or a rule didn’t run. Unlike many programming languages, generally rules with null valued variables will not evaluate that rule (and thus, downstream rules will not evaluate either). The exception cases are when nulls are explicitly checked in a rule. For example:

```python
Thing: int = JsonData(path='$.property_that_doesnt_exist')

# Evaluates to False
MyFirstRule = Rule(when_all=[
    Thing != Null,
])

# Skips evaluation and sets to Null
MySecondRule = Rule(when_all=[
    Thing > 1,
])

# Skips evaluation and sets to Null
MyThirdRule = Rule(when_all=[
    MySecondRule,
])
```

## Workflow Structure and File Placement

SML files can be composed to make your rules easier to understand. The `Import` statement allows you to include rules and variables found in other files.

```python
# models/action_name.sml
ActionName = "foo"

# main.sml
Import(
    rules=[
        'models/action_name.sml',
        'models/http_request.sml',
    ]
)

MyRule = Rule(when_all=[ActionName == "foo"])
```

`Require` allows you to selectively run other SML scripts. Requires supports templating and conditionals, allowing scripts to be filtered out if necessary. This is important in situations where some rules or UDFs are particularly expensive to run (such as making a call to an AI service, for example).

```python
# main.sml
Require(rule=f'actions/{ActionName}.sml')  # will execute 'actions/foo.sml'

Require(rule='ai_services/my_ai_service.sml', require_if=ActionName == "register")
```
