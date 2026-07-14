import { describe, expect, it } from '@rstest/core';

import { AskStreamError, frameToEvent, parseSseFrames, streamAsk } from './sseClient';
import { AskEndpointConfig, AskEvent, AskRequest } from './types';

const ENDPOINT: AskEndpointConfig = { url: 'http://test/ask' };
const REQ: AskRequest = { message: 'hi' };

function frame(type: string, payload: Record<string, unknown> = {}): string {
  return `event: ${type}\ndata: ${JSON.stringify({ version: 1, type, payload })}\n\n`;
}

function streamResponse(chunks: string[], status = 200): Response {
  const encoder = new TextEncoder();
  const body = new ReadableStream<Uint8Array>({
    start(controller) {
      for (const chunk of chunks) {
        controller.enqueue(encoder.encode(chunk));
      }
      controller.close();
    },
  });
  return new Response(body, { status });
}

function setFetch(fn: () => Promise<Response>): void {
  globalThis.fetch = fn as unknown as typeof globalThis.fetch;
}

async function collect(gen: AsyncGenerator<AskEvent>): Promise<AskEvent[]> {
  const out: AskEvent[] = [];
  for await (const event of gen) {
    out.push(event);
  }
  return out;
}

describe('parseSseFrames', () => {
  it('splits complete frames and keeps a partial remainder', () => {
    const { frames, rest } = parseSseFrames('event: a\ndata: 1\n\nevent: b\ndata: 2\n\nevent: c\ndata:');
    expect(frames).toEqual(['event: a\ndata: 1', 'event: b\ndata: 2']);
    expect(rest).toBe('event: c\ndata:');
  });

  it('does not lose or duplicate a frame split across chunks', () => {
    let buffer = '';
    const seen: string[] = [];
    for (const chunk of ['event: a\nda', 'ta: 1\n\nev', 'ent: b\ndata: 2\n\n']) {
      buffer += chunk;
      const out = parseSseFrames(buffer);
      buffer = out.rest;
      seen.push(...out.frames);
    }
    expect(seen).toEqual(['event: a\ndata: 1', 'event: b\ndata: 2']);
    expect(buffer).toBe('');
  });
});

describe('frameToEvent', () => {
  it('parses a valid frame', () => {
    const event = frameToEvent('event: done\ndata: {"version":1,"type":"done","payload":{}}');
    expect(event.type).toBe('done');
    expect(event.version).toBe(1);
  });

  it('throws on malformed json', () => {
    expect(() => frameToEvent('event: x\ndata: not-json')).toThrow(AskStreamError);
  });

  it('rejects an unknown event type (forward-compatible error)', () => {
    expect(() => frameToEvent('event: nope\ndata: {"version":1,"type":"nope","payload":{}}')).toThrow(AskStreamError);
  });
});

describe('streamAsk', () => {
  it('yields events and stops at the terminal', async () => {
    setFetch(async () =>
      streamResponse([
        frame('conversation_started'),
        frame('assistant_message', { text: 'hi' }),
        frame('done'),
        frame('assistant_message', { text: 'should be ignored after done' }),
      ])
    );
    const events = await collect(streamAsk(ENDPOINT, REQ));
    expect(events.map((e) => e.type)).toEqual(['conversation_started', 'assistant_message', 'done']);
  });

  it('errors when the stream ends without a terminal event', async () => {
    setFetch(async () => streamResponse([frame('conversation_started')]));
    await expect(collect(streamAsk(ENDPOINT, REQ))).rejects.toThrow(AskStreamError);
  });

  it('maps a pre-flight HTTP error to the server code', async () => {
    setFetch(
      async () => new Response(JSON.stringify({ error: { code: 'invalid_model', message: 'nope' } }), { status: 400 })
    );
    await expect(collect(streamAsk(ENDPOINT, REQ))).rejects.toMatchObject({ code: 'invalid_model' });
  });

  it('cancels the reader when iteration stops early', async () => {
    let cancelled = false;
    const encoder = new TextEncoder();
    const body = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(encoder.encode(frame('conversation_started')));
      },
      cancel() {
        cancelled = true;
      },
    });
    setFetch(async () => new Response(body, { status: 200 }));
    const gen = streamAsk(ENDPOINT, REQ);
    await gen.next();
    await gen.return(undefined);
    expect(cancelled).toBe(true);
  });
});
