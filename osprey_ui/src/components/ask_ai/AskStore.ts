// Zustand conversation store for the Ask panel.
//
// createAskStore(deps) is exported so tests can inject a fake `stream`; the panel and
// app use the default useAskStore. A monotonic requestId guards against stale responses
// (a slow turn cannot mutate a newer conversation), and an AbortController cancels the
// in-flight fetch/reader. Errors are mapped to a safe {code,message} -- never a raw stack.

import create from 'zustand';

import { streamAsk } from './sseClient';
import {
  AskEndpointConfig,
  AskEvent,
  AskEvidence,
  AskMessage,
  AskRequest,
  AskStatus,
  ErrorPayload,
  QueryResultPayload,
} from './types';

let _counter = 0;

function uid(): string {
  _counter += 1;
  return `m${_counter}-${Date.now().toString(36)}`;
}

function toErrorPayload(e: unknown): ErrorPayload {
  if (e && typeof e === 'object' && 'code' in e && 'message' in e) {
    const code = (e as { code: unknown }).code;
    const message = (e as { message: unknown }).message;
    if (typeof code === 'string' && typeof message === 'string') {
      return { code, message };
    }
  }
  return { code: 'internal', message: 'Something went wrong.' };
}

function evidenceFromPayload(payload: QueryResultPayload): AskEvidence {
  return {
    toolCallId: typeof payload.tool_call_id === 'string' ? payload.tool_call_id : undefined,
    name: typeof payload.name === 'string' ? payload.name : undefined,
    isError: payload.is_error === true,
    content: typeof payload.content === 'string' ? payload.content : '',
    raw: payload,
  };
}

function dropLastTurn(messages: AskMessage[]): AskMessage[] {
  // Remove the trailing assistant message and the user message before it, so retry does
  // not duplicate the user bubble when it re-sends.
  const next = [...messages];
  if (next.length > 0 && next[next.length - 1].role === 'assistant') {
    next.pop();
  }
  if (next.length > 0 && next[next.length - 1].role === 'user') {
    next.pop();
  }
  return next;
}

export interface AskDeps {
  stream: typeof streamAsk;
}

export interface AskState {
  status: AskStatus;
  endpoint?: AskEndpointConfig;
  model?: string;
  conversationId?: string;
  messages: AskMessage[];
  error?: ErrorPayload;
  configure(endpoint: AskEndpointConfig, model?: string): void;
  sendMessage(text: string, opts?: { contextRef?: string }): Promise<void>;
  retry(): Promise<void>;
  newChat(): void;
  reset(): void;
  abort(): void;
}

export function createAskStore(deps: AskDeps = { stream: streamAsk }) {
  let requestId = 0;
  let controller: AbortController | undefined;
  let lastUserText: string | undefined;
  let lastContextRef: string | undefined;

  return create<AskState>((set, get) => {
    const applyEvent = (assistantId: string, event: AskEvent): void => {
      switch (event.type) {
        case 'conversation_started':
          if (typeof event.conversation_id === 'string') {
            set({ conversationId: event.conversation_id });
          }
          break;
        case 'query_result':
          set((s) => ({
            messages: s.messages.map((m) =>
              m.id === assistantId
                ? { ...m, evidence: [...m.evidence, evidenceFromPayload(event.payload as QueryResultPayload)] }
                : m
            ),
          }));
          break;
        case 'assistant_message': {
          const raw = event.payload.text;
          const text = typeof raw === 'string' ? raw : '';
          set((s) => ({ messages: s.messages.map((m) => (m.id === assistantId ? { ...m, text } : m)) }));
          break;
        }
        case 'done':
          set((s) => ({
            status: 'idle',
            messages: s.messages.map((m) => (m.id === assistantId ? { ...m, streaming: false } : m)),
          }));
          break;
        case 'error': {
          const err = toErrorPayload(event.payload);
          set((s) => ({
            status: 'error',
            error: err,
            messages: s.messages.map((m) => (m.id === assistantId ? { ...m, streaming: false, error: err } : m)),
          }));
          break;
        }
        case 'tool_call':
        default:
          break;
      }
    };

    const runTurn = async (text: string, contextRef: string | undefined, appendUser: boolean): Promise<void> => {
      const endpoint = get().endpoint;
      if (!endpoint) {
        throw new Error('AskStore is not configured; call configure() first.');
      }
      controller?.abort();
      requestId += 1;
      const myId = requestId;
      controller = new AbortController();
      lastUserText = text;
      lastContextRef = contextRef;
      const assistantId = uid();
      set((s) => {
        const withUser = appendUser
          ? [...s.messages, { id: uid(), role: 'user' as const, text, evidence: [], streaming: false }]
          : s.messages;
        return {
          status: 'streaming' as const,
          error: undefined,
          messages: [
            ...withUser,
            { id: assistantId, role: 'assistant' as const, text: '', evidence: [], streaming: true },
          ],
        };
      });
      const req: AskRequest = {
        message: text,
        conversation_id: get().conversationId,
        model: get().model,
        context_ref: contextRef,
      };
      try {
        for await (const event of deps.stream(endpoint, req, { signal: controller.signal })) {
          if (myId !== requestId) {
            return; // a newer send/newChat/reset superseded this turn
          }
          applyEvent(assistantId, event);
        }
      } catch (e) {
        if (myId !== requestId || controller?.signal.aborted) {
          return; // stale or user-aborted: do not surface an error
        }
        const err = toErrorPayload(e);
        set((s) => ({
          status: 'error',
          error: err,
          messages: s.messages.map((m) => (m.id === assistantId ? { ...m, streaming: false, error: err } : m)),
        }));
      }
    };

    return {
      status: 'idle',
      messages: [],
      configure: (endpoint, model) => set({ endpoint, model }),
      abort: () => {
        controller?.abort();
        controller = undefined;
        requestId += 1;
        if (get().status === 'streaming') {
          set({ status: 'idle' });
        }
      },
      newChat: () => {
        requestId += 1;
        controller?.abort();
        controller = undefined;
        set({ conversationId: undefined, messages: [], status: 'idle', error: undefined });
      },
      reset: () => {
        requestId += 1;
        controller?.abort();
        controller = undefined;
        lastUserText = undefined;
        lastContextRef = undefined;
        set({ conversationId: undefined, messages: [], status: 'idle', error: undefined });
      },
      retry: async () => {
        if (get().status !== 'error' || lastUserText === undefined) {
          return;
        }
        set((s) => ({ status: 'idle', error: undefined, messages: dropLastTurn(s.messages) }));
        await runTurn(lastUserText, lastContextRef, true);
      },
      sendMessage: async (text, opts) => {
        await runTurn(text, opts?.contextRef, true);
      },
    };
  });
}

export const useAskStore = createAskStore();
