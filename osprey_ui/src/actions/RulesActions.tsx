import HTTPUtils, { HTTPResponse } from '../utils/HTTPUtils';
import {
  ParseIntoBuilderResponse,
  PendingDraftsResponse,
  RuleDraftSourceResponse,
  RuleDraftSubmitResponse,
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

export interface SubmitRuleDraftBody {
  path: string;
  source: string;
  rule_name: string;
  summary: string;
  is_new_rule: boolean;
  wire_into_main?: boolean;
  branch?: string;
}

export async function submitRuleDraft(body: SubmitRuleDraftBody): Promise<RuleDraftSubmitResponse> {
  const response: HTTPResponse = await HTTPUtils.post('rule-drafts/submit', body);
  if (response.ok) {
    return response.data;
  }
  const errPayload = response.error.response?.data as { error?: string } | undefined;
  throw new Error(errPayload?.error ?? response.error.message ?? 'Failed to submit rule draft');
}

export async function getPendingRuleDrafts(): Promise<PendingDraftsResponse> {
  // Returns an empty list rather than throwing on failure so the RulesPage still renders
  // when GitHub isn't configured.
  const response: HTTPResponse = await HTTPUtils.get('rule-drafts/pending');
  if (response.ok) {
    return response.data;
  }
  const errPayload = response.error.response?.data as PendingDraftsResponse | undefined;
  if (errPayload && Array.isArray(errPayload.pending)) {
    return errPayload;
  }
  return { pending: [], error: response.error.message ?? 'Failed to fetch pending drafts' };
}
