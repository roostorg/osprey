# Labels

Labels are annotations you apply to entities: users, IP addresses, emails, and other tracked objects. They're the bridge between human judgment and Osprey's automated rule system; a label you apply manually can feed into rules that act on future events automatically.

Labels have one of three connotations:

- **Negative**: harmful or problematic (e.g. `spammer`, `bot`, `banned`, `suspicious`)
- **Positive**: trusted or verified (e.g. `verified`, `trusted`, `premium_user`)
- **Neutral**: informational (e.g. `new_user`, `from_mobile`, `beta_tester`)

A reason is required whenever you apply a label.

## Entity Details

Selecting an entity anywhere in the UI navigates to an entity view showing every label that has ever been applied to that entity, grouped by label name.

![The entity view for a user, listing their negative labels—including identity_evasion with its description and an automatically added RapidHandleChange entry—beside the query page's charts and event stream](../../images/osprey-user-entity.png)

Each label entry shows:
- The label value and type
- Whether it was applied by a rule (automated), manually, or via a bulk action
- The reason provided
- Who applied it and when

## Add manually

From a Top N table, hover over an entity row and select **Edit Labels**:

![The label popup for a PostText entity in a Top N table, with keyboard hints for adding the entity to the query and an Edit Labels button](../../images/add-labels.png)

From the event stream, select any entity to open its label drawer.

![The label drawer for a user entity, with a label name search, reason and expiration fields, and empty negative, positive, and neutral label sections](../../images/empty-label.png)

![An entity view with negative labels applied, one expanded to show its description and the rule event that produced it, plus neutral labels below; identifying values are redacted](../../images/complete-label.png)

For more about how labels are used, see [Writing Rules → Labels](../../rules/#labels).
