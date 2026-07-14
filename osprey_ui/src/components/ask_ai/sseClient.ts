// Chunk-safe POST-SSE client for the Ask endpoint.
//
// Uses fetch + a ReadableStream reader + TextDecoder to consume `event:`/`data:` frames
// separated by a blank line, tolerant of arbitrary chunk boundaries. Abortable via an
// AbortSignal; the reader is always cancelled on return/throw. A pre-flight HTTP error
// (the backend's JSON `{error:{code,message}}`) becomes an AskStreamError; a stream that
// ends without a terminal event is an error; unknown event types are rejected rather
// than silently interpreted.

import { AskEndpointConfig, AskEvent, AskEventType, AskRequest, ErrorPayload } from './types';

export class AskStreamError extends Error {
  code: string;

  constructor(code: string, message: string) {
    super(message);
    this.name = 'AskStreamError';
    this.code = code;
  }
}

const TERMINAL: ReadonlySet<AskEventType> = new Set<AskEventType>(['done', 'error']);
const KNOWN_TYPES: ReadonlySet<string> = new Set<AskEventType>([
  'conversation_started',
  'tool_call',
  'query_result',
  'assistant_message',
  'done',
  'error',
]);

// Pure: split a buffer into complete frames plus the trailing (possibly partial) remainder.
export function parseSseFrames(buffer: string): { frames: string[]; rest: string } {
  const parts = buffer.split('\n\n');
  const rest = parts.pop() ?? '';
  return { frames: parts.filter((part) => part.trim() !== ''), rest };
}

// Pure: parse one raw frame into a validated AskEvent (throws on malformed/unknown type).
export function frameToEvent(frame: string): AskEvent {
  let data = '';
  for (const line of frame.split('\n')) {
    if (line.startsWith('data:')) {
      data += line.slice(5).replace(/^ /, '');
    }
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(data);
  } catch {
    throw new AskStreamError('internal', 'Malformed event frame.');
  }
  const type = (parsed as { type?: unknown } | null)?.type;
  if (typeof parsed !== 'object' || parsed === null || typeof type !== 'string' || !KNOWN_TYPES.has(type)) {
    throw new AskStreamError('internal', 'Unknown or malformed event.');
  }
  return parsed as AskEvent;
}

async function toHttpError(res: Response): Promise<AskStreamError> {
  let body: { error?: Partial<ErrorPayload> } | undefined;
  try {
    body = (await res.json()) as { error?: Partial<ErrorPayload> };
  } catch {
    return new AskStreamError('internal', `Request failed (${res.status}).`);
  }
  const err = body?.error;
  if (err && typeof err.code === 'string' && typeof err.message === 'string') {
    return new AskStreamError(err.code, err.message);
  }
  return new AskStreamError('internal', `Request failed (${res.status}).`);
}

export interface StreamOptions {
  signal?: AbortSignal;
}

export async function* streamAsk(
  endpoint: AskEndpointConfig,
  req: AskRequest,
  opts: StreamOptions = {},
): AsyncGenerator<AskEvent> {
  const res = await fetch(endpoint.url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ...(endpoint.headers ?? {}) },
    body: JSON.stringify(req),
    signal: opts.signal,
    credentials: endpoint.withCredentials ? 'include' : 'same-origin',
  });
  if (!res.ok) {
    throw await toHttpError(res);
  }
  if (!res.body) {
    throw new AskStreamError('internal', 'Response had no body to stream.');
  }

  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  let buffer = '';
  let sawTerminal = false;
  try {
    for (let result = await reader.read(); !result.done; result = await reader.read()) {
      buffer += decoder.decode(result.value, { stream: true });
      const { frames, rest } = parseSseFrames(buffer);
      buffer = rest;
      for (const frame of frames) {
        const event = frameToEvent(frame);
        if (TERMINAL.has(event.type)) {
          sawTerminal = true;
        }
        yield event;
        if (sawTerminal) {
          return;
        }
      }
    }
    if (!sawTerminal) {
      throw new AskStreamError('internal', 'Stream ended without a terminal event.');
    }
  } finally {
    await reader.cancel().catch(() => undefined);
  }
}
