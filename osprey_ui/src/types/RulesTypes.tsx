export interface RuleInfo {
  name: string;
  source_file: string;
  description: string;
  when_all: string[];
  referenced_features: string[];
  referenced_by_whenrules: number;
}

export interface RulesListResponse {
  rules: RuleInfo[];
  total: number;
  when_rules_total: number;
  unused_total: number;
}

export enum SortKey {
  Name = 'name',
  MostReferenced = 'most-referenced',
  LeastReferenced = 'least-referenced',
}

export interface RuleDraftValidationMessage {
  message: string;
  hint?: string | null;
  source_path: string;
  line: number;
  column: number;
  rendered: string;
  identifier?: string | null;
  defined_in_source_paths?: string[];
}

export interface RuleDraftValidationResponse {
  ok: boolean;
  errors: RuleDraftValidationMessage[];
  warnings: RuleDraftValidationMessage[];
  suggested_imports?: string[];
  // Set when the engine's sources couldn't be assembled at all (e.g. a broken
  // main.sml), which is distinct from this draft's SML failing validation — nothing
  // the author types will fix it.
  assemble_error?: string | null;
}

export interface RuleSourceResponse {
  path: string;
  contents: string;
}

export interface RuleVocabularyFeature {
  name: string;
  source_path: string;
  source_line: number;
}

export interface RuleVocabularyUdfArgument {
  name: string;
  type_name: string;
}

export interface RuleVocabularyUdf {
  name: string;
  return_type: string;
  arguments: RuleVocabularyUdfArgument[];
}

export interface RuleVocabulary {
  features: RuleVocabularyFeature[];
  udfs: RuleVocabularyUdf[];
  effects: string[];
  source_files: string[];
}

// `deploy_requested` exists because CAN_EDIT_RULES alone can write a draft but not ship
// it. Without it the table shows `draft` for both work in progress and work waiting on
// someone else. Reachable from `deployed` too, which reads as "a redeploy is wanted";
// `deployed_at` stays set, so the two remain distinguishable.
export type RuleRecordStatus = 'draft' | 'deploy_requested' | 'deployed';

// A row of the rules table without its SML — what `GET /rules/drafts` lists. Named for
// the row rather than for `draft` because a row is a rule at some point in its lifecycle:
// deploying marks it deployed, and editing a deployed rule returns it to draft. `status`
// is the only thing separating the two, so there is no distinct deployed-rule type.
//
// This is the shape anything rendering *about* a rule should ask for. Only the editor
// needs the SML, and it opens one draft at a time.
export interface RuleDraftSummary {
  // A string on the wire, not a number. Osprey mints ids as snowflakes in places, and a
  // 64-bit id exceeds JavaScript's exact integer range — parsing one as a number would
  // silently drop its low bits. Pass it through as an opaque string.
  id: string;
  path: string;
  rule_name: string;
  // Content address of the SML: SHA-256 of its UTF-8 bytes, hex, with no normalisation —
  // so it answers "is this the same text?", not "is this the same program". Deploy writes
  // the source verbatim, which is what lets the server tell a rule that is deployed and
  // current from one edited on disk since, without keeping a second copy to diff.
  //
  // Not used for the editor's own dirty check: that holds the source already, and a
  // direct comparison is exact and synchronous where hashing would be neither.
  cid: string;
  author: string;
  status: RuleRecordStatus;
  // The list is ordered by this.
  updated_at: string | null;
}

// A full row: the summary plus the SML itself. Served where one specific draft is the
// subject — fetching it into the editor, and the responses to creating or deploying it.
//
// Extends rather than duplicates, mirroring the API, so anything needing only summary
// fields can be typed `RuleDraftSummary` and still accept a full record.
export interface RuleRecord extends RuleDraftSummary {
  source: string;
  summary: string;
  created_at: string | null;
  deployed_at: string | null;
}

export interface RuleDraftsListResponse {
  drafts: RuleDraftSummary[];
}

// `GET /rules/drafts/<id>/deploy-plan` — what a deploy would actually do, computed by the
// side that can see the rules directory. The client cannot derive any of this: it never
// sees the filesystem, and `cid` only answers "is this the same text?" once something has
// hashed the file to compare against.
export type RuleSourcePlanState = 'valid' | 'invalid';
export type RuleFilePlanState = 'new' | 'identical' | 'differs';
export type MainSmlPlanState = 'would_append' | 'already_required' | 'missing' | 'unparseable';

export interface RuleDeployPlan {
  // Whether the draft still compiles against the engine's current sources.
  source: { state: RuleSourcePlanState };
  rule_file: { path: string; state: RuleFilePlanState };
  main_sml: {
    state: MainSmlPlanState;
    // The exact line deploy would add, when `state` is `would_append` — so the dialog can
    // show what it writes rather than paraphrase it.
    require_line: string | null;
  };
  // Two independent facts, not one verdict. `deployable` is about writing the rule file:
  // the draft compiles and the rules directory is usable. `wireable_into_main` is about
  // the Require line: main.sml exists and parses. A broken draft with a fine main.sml is
  // `false, true`; a good draft with a broken main.sml is the reverse.
  //
  // Deploy is possible when `deployable && (!wireIntoMain || wireable_into_main)`.
  deployable: boolean;
  // Server-derived rather than inferred from `main_sml.state`, so a state added later
  // disables the checkbox on an old client instead of leaving it wrongly enabled.
  wireable_into_main: boolean;
}

// The result of POST /rules/drafts/<id>/deploy. `path_on_disk` is relative to the
// configured rules directory; `main_sml_updated` says whether a `Require(...)` line was
// appended to main.sml, which is what makes the deployed file take effect at all.
export interface RuleDeployment {
  rule: RuleRecord;
  main_sml_updated: boolean;
  path_on_disk: string;
}

export type ConditionOperator = '==' | '!=' | '>' | '<' | '>=' | '<=' | 'includes' | 'excludes';

// --------------------------------------------------------------------------- //
// Rule Builder — wire shapes
//
// The API serialises every response snake_case with no exceptions. These models used to
// carry camelCase aliases and were the single exception to that, so the conversion is
// now the client's. The `Raw*` types below are what comes off the wire — mirroring
// `RawUIConfig` — and `RulesActions` converts them into the view models beneath.
// --------------------------------------------------------------------------- //

export interface RawRuleBuilderCondition {
  feature: string;
  operator: ConditionOperator;
  rhs: string;
  rhs_is_feature: boolean;
}

export interface RawRuleBuilderOutcomeArg {
  // Null for a positional argument.
  name: string | null;
  value: string;
  is_feature: boolean;
}

export interface RawRuleBuilderOutcome {
  effect: string;
  args: RawRuleBuilderOutcomeArg[];
}

export interface RawRuleBuilderModel {
  rule_name: string;
  description: string;
  conditions: RawRuleBuilderCondition[];
  outcomes: RawRuleBuilderOutcome[];
}

// The API emits all three keys every time, so `supported` is the discriminant and the
// other two are nullable rather than absent.
export interface RawParseIntoBuilderResponse {
  supported: boolean;
  reason: string | null;
  model: RawRuleBuilderModel | null;
}

// --------------------------------------------------------------------------- //
// Rule Builder — view models
// --------------------------------------------------------------------------- //

export interface RuleBuilderCondition {
  feature: string;
  operator: ConditionOperator;
  rhs: string;
  rhsIsFeature: boolean;
}

export interface RuleBuilderOutcomeArg {
  // Null for a positional argument: the SML generator emits a bare value for these
  // rather than `name=value`, so a call the parser read positionally round-trips.
  name: string | null;
  value: string;
  isFeature: boolean;
}

export interface RuleBuilderOutcome {
  effect: string;
  args: RuleBuilderOutcomeArg[];
}

export interface RuleBuilderModel {
  ruleName: string;
  description: string;
  conditions: RuleBuilderCondition[];
  outcomes: RuleBuilderOutcome[];
}

export type ParseIntoBuilderResponse =
  | { supported: true; model: RuleBuilderModel }
  | { supported: false; reason: string };
