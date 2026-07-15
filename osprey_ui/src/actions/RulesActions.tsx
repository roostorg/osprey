import HTTPUtils, { HTTPResponse } from '../utils/HTTPUtils';
import {
  DeployRuleDraftResponse,
  ParseIntoBuilderResponse,
  RuleDraft,
  RuleDraftSourceResponse,
  RuleDraftsListResponse,
  RuleDraftValidationResponse,
  RuleDraftVocabulary,
  RulesListResponse,
} from '../types/RulesTypes';

export async function getRulesList(): Promise<RulesListResponse> {
  const response: HTTPResponse = await HTTPUtils.get('rules');
  if (response.ok) {
    return response.data;
  }
  throw new Error(response.error.message ?? 'Failed to fetch rules list');
}

export async function getRuleDraftSource(path: string): Promise<RuleDraftSourceResponse> {
  const response: HTTPResponse = await HTTPUtils.get('rule-drafts/source', { params: { path } });
  if (response.ok) {
    return response.data;
  }
  throw new Error(response.error.message ?? `Failed to fetch rule source at ${path}`);
}

export async function validateRuleDraft(path: string, source: string): Promise<RuleDraftValidationResponse> {
  // SML validation errors come back as 200 with {ok: false}; backend-shape problems come back as 400
  // with the same envelope. Both paths surface the structured errors to the UI without throwing.
  const response: HTTPResponse = await HTTPUtils.post('rule-drafts/validate', { path, source });
  if (response.ok) {
    return response.data;
  }
  if (response.error.response?.data) {
    return response.error.response.data as RuleDraftValidationResponse;
  }
  throw new Error(response.error.message ?? 'Validation request failed');
}

export async function parseRuleDraftIntoBuilder(path: string, source: string): Promise<ParseIntoBuilderResponse> {
  const response: HTTPResponse = await HTTPUtils.post('rule-drafts/parse-into-builder', { path, source });
  if (response.ok) {
    return response.data;
  }
  throw new Error(response.error.message ?? 'Failed to parse rule into builder model');
}

export async function getRuleDraftVocabulary(): Promise<RuleDraftVocabulary> {
  const response: HTTPResponse = await HTTPUtils.get('rule-drafts/vocabulary');
  if (response.ok) {
    return response.data;
  }
  throw new Error(response.error.message ?? 'Failed to fetch rule vocabulary');
}

export interface CreateRuleDraftBody {
  path: string;
  source: string;
  rule_name: string;
  summary: string;
}

// Saves a draft into the rule_drafts table (upserted by path). The draft is staged,
// not live; deployRuleDraft writes it into the rules directory.
export async function createRuleDraft(body: CreateRuleDraftBody): Promise<RuleDraft> {
  const response: HTTPResponse = await HTTPUtils.post('rule-drafts', body);
  if (response.ok) {
    return response.data;
  }
  const errPayload = response.error.response?.data as { error?: string } | undefined;
  throw new Error(errPayload?.error ?? response.error.message ?? 'Failed to save rule draft');
}

export interface DeployRuleDraftBody {
  // Also append a Require line to main.sml so the rule takes effect.
  wire_into_main?: boolean;
}

export async function deployRuleDraft(id: number, body: DeployRuleDraftBody = {}): Promise<DeployRuleDraftResponse> {
  const response: HTTPResponse = await HTTPUtils.post(`rule-drafts/${id}/deploy`, body);
  if (response.ok) {
    return response.data;
  }
  const errPayload = response.error.response?.data as { error?: string } | undefined;
  throw new Error(errPayload?.error ?? response.error.message ?? 'Failed to deploy rule draft');
}

export async function getRuleDrafts(): Promise<RuleDraftsListResponse> {
  // Returns an empty list rather than throwing on failure so the RulesPage still renders
  // when the caller lacks the rule-drafts ability.
  const response: HTTPResponse = await HTTPUtils.get('rule-drafts');
  if (response.ok) {
    return response.data;
  }
  const errPayload = response.error.response?.data as RuleDraftsListResponse | undefined;
  if (errPayload && Array.isArray(errPayload.drafts)) {
    return errPayload;
  }
  return { drafts: [] };
}
