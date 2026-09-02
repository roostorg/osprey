import HTTPUtils, { HTTPResponse } from '../utils/HTTPUtils';
import {
  ParseIntoBuilderResponse,
  RawParseIntoBuilderResponse,
  RawRuleBuilderModel,
  RuleBuilderModel,
  RuleDeployPlan,
  RuleDeployment,
  RuleDraftValidationResponse,
  RuleDraftsListResponse,
  RuleRecord,
  RuleSourceResponse,
  RuleVocabulary,
  RulesListResponse,
} from '../types/RulesTypes';

export async function getRulesList(): Promise<RulesListResponse> {
  const response: HTTPResponse = await HTTPUtils.get('rules');
  if (response.ok) {
    return response.data;
  }
  throw new Error(errorMessageFrom(response) ?? 'Failed to fetch rules list');
}

export async function getRuleSource(path: string): Promise<RuleSourceResponse> {
  const response: HTTPResponse = await HTTPUtils.get('rules/source', { params: { path } });
  if (response.ok) {
    return response.data;
  }
  throw new Error(errorMessageFrom(response) ?? `Failed to fetch rule source at ${path}`);
}

// `signal` lets a caller abandon a validation that has been superseded. The editor
// re-validates as you type, and validating means assembling and checking the engine's
// whole source set server-side — so a request nobody is waiting for any more is worth
// actually cancelling, not just ignoring the reply to.
export async function validateRuleDraft(
  path: string,
  source: string,
  signal?: AbortSignal
): Promise<RuleDraftValidationResponse> {
  // SML validation errors come back as 200 with {ok: false}; backend-shape problems come back as 400
  // with the same envelope. Both paths surface the structured errors to the UI without throwing.
  const response: HTTPResponse = await HTTPUtils.post('rules/drafts/validate', { path, source }, { signal });
  if (response.ok) {
    return response.data;
  }
  if (response.error.response?.data) {
    return response.error.response.data as RuleDraftValidationResponse;
  }
  throw new Error(response.error.message ?? 'Validation request failed');
}

// The builder models are the one place the wire shape and the view shape differ: the API
// serialises snake_case throughout, and these used to be the sole endpoint emitting
// camelCase aliases. Converting here keeps snake_case out of the editor's state.
function toBuilderModel(raw: RawRuleBuilderModel): RuleBuilderModel {
  return {
    ruleName: raw.rule_name,
    description: raw.description,
    conditions: raw.conditions.map((c) => {
      return { feature: c.feature, operator: c.operator, rhs: c.rhs, rhsIsFeature: c.rhs_is_feature };
    }),
    outcomes: raw.outcomes.map((o) => {
      return {
        effect: o.effect,
        args: o.args.map((arg) => {
          return { name: arg.name, value: arg.value, isFeature: arg.is_feature };
        }),
      };
    }),
  };
}

export async function parseRuleDraftIntoBuilder(path: string, source: string): Promise<ParseIntoBuilderResponse> {
  const response: HTTPResponse = await HTTPUtils.post('rules/drafts/parse', { path, source });
  if (!response.ok) {
    throw new Error(errorMessageFrom(response) ?? 'Failed to parse rule into builder model');
  }
  const raw: RawParseIntoBuilderResponse = response.data;
  // "Can't be represented" is an answer, not a failure — the editor uses it to decide
  // whether to offer the Rule Builder toggle at all.
  if (!raw.supported || raw.model == null) {
    return { supported: false, reason: raw.reason ?? 'This file uses SML the Rule Builder cannot represent.' };
  }
  return { supported: true, model: toBuilderModel(raw.model) };
}

export async function getRuleVocabulary(): Promise<RuleVocabulary> {
  const response: HTTPResponse = await HTTPUtils.get('rules/vocabulary');
  if (response.ok) {
    return response.data;
  }
  throw new Error(errorMessageFrom(response) ?? 'Failed to fetch rule vocabulary');
}

export interface CreateRuleDraftBody {
  path: string;
  source: string;
  rule_name: string;
  summary: string;
}

// A refused save is an outcome the editor renders, not an exception: 422 (SML that
// doesn't compile, or main.sml as the target) and 409 (the rule name belongs to another
// draft) both answer with the same `DraftValidation` envelope `validateRuleDraft`
// returns, so the editor can show them through the validation panel it already has.
export type CreateRuleDraftResult =
  | { ok: true; draft: RuleRecord }
  | { ok: false; validation: RuleDraftValidationResponse };

// Saves a draft into the rules table (upserted by path). The draft is staged for someone
// with the deploy ability to review; saving never changes any live rules.
export async function createRuleDraft(body: CreateRuleDraftBody): Promise<CreateRuleDraftResult> {
  const response: HTTPResponse = await HTTPUtils.post('rules/drafts', body);
  if (response.ok) {
    return { ok: true, draft: response.data };
  }
  const validation = draftValidationFrom(response);
  if (validation != null) {
    return { ok: false, validation };
  }
  throw new Error(errorMessageFrom(response) ?? 'Failed to save rule draft');
}

// Loads a single draft (its SML lives in the table, not on disk, so editing a draft
// reads it from here rather than from the rules directory).
export async function getRuleDraft(id: string): Promise<RuleRecord> {
  const response: HTTPResponse = await HTTPUtils.get(`rules/drafts/${encodeURIComponent(id)}`);
  if (response.ok) {
    return response.data;
  }
  throw new Error(errorMessageFrom(response) ?? 'Failed to load rule draft');
}

export async function getRuleDrafts(): Promise<RuleDraftsListResponse> {
  // Returns an empty list rather than throwing on failure so the RulesPage still renders
  // for someone who can view rules but not edit them — listing drafts needs CAN_EDIT_RULES.
  const response: HTTPResponse = await HTTPUtils.get('rules/drafts');
  if (response.ok) {
    return response.data;
  }
  const errPayload = response.error.response?.data as RuleDraftsListResponse | undefined;
  if (errPayload && Array.isArray(errPayload.drafts)) {
    return errPayload;
  }
  return { drafts: [] };
}

// What a deploy would do, according to the side that can read the rules directory.
// Returns null rather than throwing when the endpoint is unavailable — a deployment
// running a build without it should still be able to deploy, falling back to a plan that
// says what the API contract guarantees instead of what the disk currently holds.
// Takes no wiring argument: the plan answers for both choices at once, so the dialog's
// checkbox re-renders from a plan it already holds rather than re-requesting one.
export async function getDeployPlan(id: string): Promise<RuleDeployPlan | null> {
  const response: HTTPResponse = await HTTPUtils.get(`rules/drafts/${encodeURIComponent(id)}/deploy-plan`, {
    // Every status counts as a resolution rather than a rejection. A deployment whose
    // build predates this endpoint answers 404, and the shared interceptor would file
    // that in the global error store — where an unrelated page later renders it as a
    // real failure. An absent optional endpoint is not an error worth reporting.
    validateStatus: () => true,
  });
  if (!response.ok || response.status !== 200) return null;
  return response.data;
}

// Marks a draft as ready for someone who can deploy to pick up. Needs CAN_EDIT_RULES, not
// CAN_DEPLOY_RULES — it exists precisely for authors who cannot deploy. Idempotent, and
// editing the draft afterwards returns it to `draft`, because the request was for
// particular text and the text changed.
export async function requestDeploy(id: string): Promise<RuleRecord> {
  const response: HTTPResponse = await HTTPUtils.post(`rules/drafts/${encodeURIComponent(id)}/request-deploy`, {});
  if (response.ok) {
    return response.data;
  }
  throw new Error(errorMessageFrom(response) ?? 'Failed to request deployment');
}

export type DeployRuleDraftResult =
  | { ok: true; deployment: RuleDeployment }
  | { ok: false; validation: RuleDraftValidationResponse };

// Writes the draft's SML into the configured rules directory and marks the row deployed.
// With `wireIntoMain`, also appends a `Require(...)` line to main.sml — without which the
// deployed file sits on disk inert.
export async function deployRuleDraft(id: string, wireIntoMain: boolean): Promise<DeployRuleDraftResult> {
  const response: HTTPResponse = await HTTPUtils.post(`rules/drafts/${encodeURIComponent(id)}/deploy`, {
    wire_into_main: wireIntoMain,
  });
  if (response.ok) {
    return { ok: true, deployment: response.data };
  }
  // 422 means the stored SML no longer validates against the engine's current sources,
  // and answers with the same envelope as validation. Everything else deploy can refuse
  // with — 404 no such draft, 409 a main.sml that is missing or doesn't parse, 503 no
  // rules directory — answers `{error: ...}` and has nothing to attach to a source path.
  const validation = draftValidationFrom(response);
  if (validation != null) {
    return { ok: false, validation };
  }
  throw new Error(errorMessageFrom(response) ?? 'Failed to deploy rule draft');
}

// The API answers a refusal one of three ways: `{error: ...}` for 404/409/503, a
// `DraftValidation` for 422 and for a name collision, and the request validator's own
// error array for a body it rejected outright (400). Prefer whichever the response
// actually holds over axios's generic "Request failed with status code 4xx".
function errorMessageFrom(response: HTTPResponse): string | undefined {
  if (response.ok) return undefined;
  const data = response.error.response?.data;
  if (data != null && typeof data === 'object' && 'error' in data) {
    const { error } = data as { error?: unknown };
    if (typeof error === 'string') return error;
  }
  const validation = draftValidationFrom(response);
  const firstError = validation?.errors[0]?.message;
  if (firstError != null) return firstError;
  return response.error.message ?? undefined;
}

function draftValidationFrom(response: HTTPResponse): RuleDraftValidationResponse | undefined {
  if (response.ok) return undefined;
  const data = response.error.response?.data;
  if (data == null || typeof data !== 'object') return undefined;
  const candidate = data as Partial<RuleDraftValidationResponse>;
  if (typeof candidate.ok !== 'boolean' || !Array.isArray(candidate.errors)) return undefined;
  return { ...candidate, ok: candidate.ok, errors: candidate.errors, warnings: candidate.warnings ?? [] };
}
