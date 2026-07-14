// Vendor-neutral Ask AI wire + domain types.
//
// Wire fields (AskRequest, AskEvent, *Payload) are snake_case to match the JSON the
// backend emits and the existing Osprey UI type conventions. Domain types the store
// owns (AskEvidence, AskMessage) are camelCase.

export const ASK_EVENT_VERSION = 1;

export type AskEventType =
  | 'conversation_started'
  | 'tool_call'
  | 'query_result'
  | 'assistant_message'
  | 'done'
  | 'error';

export interface AskRequest {
  message: string;
  conversation_id?: string;
  model?: string;
  context_ref?: string;
}

export interface AskEvent {
  version: number;
  type: AskEventType;
  conversation_id?: string;
  turn_id?: string;
  payload: Record<string, unknown>;
}

export interface ToolCallPayload {
  id: string;
  name: string;
  arguments: Record<string, unknown>;
}

export interface QueryResultPayload {
  tool_call_id?: string;
  name?: string;
  is_error?: boolean;
  content?: string;
  [key: string]: unknown;
}

export interface AssistantMessagePayload {
  text: string;
}

export interface ErrorPayload {
  code: string;
  message: string;
}

export interface AskEndpointConfig {
  url: string;
  headers?: Record<string, string>;
  withCredentials?: boolean;
}

export type AskRole = 'user' | 'assistant';

export interface AskEvidence {
  toolCallId?: string;
  name?: string;
  isError: boolean;
  content: string;
  raw: QueryResultPayload;
}

export interface AskMessage {
  id: string;
  role: AskRole;
  text: string;
  evidence: AskEvidence[];
  streaming: boolean;
  error?: ErrorPayload;
}

export type AskStatus = 'idle' | 'streaming' | 'error';
