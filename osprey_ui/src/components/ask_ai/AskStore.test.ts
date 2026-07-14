import { describe, expect, it } from '@rstest/core';

import { AskDeps, createAskStore } from './AskStore';
import { AskStreamError } from './sseClient';
import { AskEndpointConfig, AskEvent } from './types';

type StreamFn = AskDeps['stream'];

const ENDPOINT: AskEndpointConfig = { url: 'http://test/ask' };

function ev(type: AskEvent['type'], payload: Record<string, unknown> = {}, extra: Partial<AskEvent> = {}): AskEvent {
  return { version: 1, type, payload, ...extra };
}

function streamOf(events: AskEvent[]): StreamFn {
  return (async function* () {
    for (const event of events) {
      yield event;
    }
  }) as unknown as StreamFn;
}

function gatedStream(before: AskEvent[], gate: Promise<void>, after: AskEvent[]): StreamFn {
  return (async function* () {
    for (const event of before) {
      yield event;
    }
    await gate;
    for (const event of after) {
      yield event;
    }
  }) as unknown as StreamFn;
}

function throwingStream(before: AskEvent[], error: unknown): StreamFn {
  return (async function* () {
    for (const event of before) {
      yield event;
    }
    throw error;
  }) as unknown as StreamFn;
}

describe('AskStore', () => {
  it('applies a happy-path stream and finishes idle', async () => {
    const useStore = createAskStore({
      stream: streamOf([
        ev('conversation_started', {}, { conversation_id: 'c1' }),
        ev('query_result', { tool_call_id: 't', name: 'lookup', is_error: false, content: 'r' }),
        ev('assistant_message', { text: 'hello' }),
        ev('done'),
      ]),
    });
    useStore.getState().configure(ENDPOINT);
    await useStore.getState().sendMessage('hi');
    const s = useStore.getState();
    expect(s.status).toBe('idle');
    expect(s.conversationId).toBe('c1');
    expect(s.messages.map((m) => m.role)).toEqual(['user', 'assistant']);
    const assistant = s.messages[1];
    expect(assistant.text).toBe('hello');
    expect(assistant.streaming).toBe(false);
    expect(assistant.evidence).toHaveLength(1);
    expect(assistant.evidence[0].content).toBe('r');
  });

  it('prevents a stale response from overwriting a newer conversation', async () => {
    let release = (): void => undefined;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const useStore = createAskStore({
      stream: gatedStream(
        [ev('conversation_started', {}, { conversation_id: 'old' })],
        gate,
        [ev('assistant_message', { text: 'late answer' }), ev('done')],
      ),
    });
    useStore.getState().configure(ENDPOINT);
    const pending = useStore.getState().sendMessage('first');
    useStore.getState().newChat();
    release();
    await pending;
    const s = useStore.getState();
    expect(s.messages).toHaveLength(0);
    expect(s.conversationId).toBeUndefined();
    expect(s.status).toBe('idle');
  });

  it('abort stops streaming without surfacing an error', async () => {
    let release = (): void => undefined;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const useStore = createAskStore({
      stream: gatedStream(
        [ev('conversation_started', {}, { conversation_id: 'c1' })],
        gate,
        [ev('assistant_message', { text: 'late' }), ev('done')],
      ),
    });
    useStore.getState().configure(ENDPOINT);
    const pending = useStore.getState().sendMessage('hi');
    useStore.getState().abort();
    release();
    await pending;
    const s = useStore.getState();
    expect(s.status).toBe('idle');
    expect(s.error).toBeUndefined();
    const assistant = s.messages.find((m) => m.role === 'assistant');
    expect(assistant?.text).toBe('');
  });

  it('maps an AskStreamError to a safe error state', async () => {
    const useStore = createAskStore({
      stream: throwingStream([ev('conversation_started', {}, { conversation_id: 'c1' })], new AskStreamError('provider_error', 'Provider unavailable.')),
    });
    useStore.getState().configure(ENDPOINT);
    await useStore.getState().sendMessage('hi');
    const s = useStore.getState();
    expect(s.status).toBe('error');
    expect(s.error?.code).toBe('provider_error');
    expect(s.error?.message).toBe('Provider unavailable.');
  });

  it('never exposes a raw thrown Error message', async () => {
    const useStore = createAskStore({
      stream: throwingStream([ev('conversation_started')], new Error('secret stack xyz')),
    });
    useStore.getState().configure(ENDPOINT);
    await useStore.getState().sendMessage('hi');
    const s = useStore.getState();
    expect(s.status).toBe('error');
    expect(s.error?.code).toBe('internal');
    expect(JSON.stringify(s)).not.toContain('xyz');
  });

  it('retry re-sends the last user turn without duplicating the user bubble', async () => {
    let attempt = 0;
    const stream: StreamFn = ((): ReturnType<StreamFn> => {
      attempt += 1;
      if (attempt === 1) {
        return (async function* () {
          yield ev('conversation_started', {}, { conversation_id: 'c1' });
          throw new AskStreamError('provider_error', 'boom');
        })();
      }
      return (async function* () {
        yield ev('assistant_message', { text: 'ok now' });
        yield ev('done');
      })();
    }) as unknown as StreamFn;
    const useStore = createAskStore({ stream });
    useStore.getState().configure(ENDPOINT);
    await useStore.getState().sendMessage('question');
    expect(useStore.getState().status).toBe('error');
    await useStore.getState().retry();
    const s = useStore.getState();
    expect(s.status).toBe('idle');
    const users = s.messages.filter((m) => m.role === 'user');
    expect(users).toHaveLength(1);
    expect(users[0].text).toBe('question');
    expect(s.messages.find((m) => m.role === 'assistant')?.text).toBe('ok now');
  });

  it('newChat clears messages but keeps configuration', async () => {
    const useStore = createAskStore({ stream: streamOf([ev('assistant_message', { text: 'a' }), ev('done')]) });
    useStore.getState().configure(ENDPOINT, 'model-x');
    await useStore.getState().sendMessage('hi');
    expect(useStore.getState().messages.length).toBeGreaterThan(0);
    useStore.getState().newChat();
    const s = useStore.getState();
    expect(s.messages).toHaveLength(0);
    expect(s.conversationId).toBeUndefined();
    expect(s.endpoint).toBeDefined();
    expect(s.model).toBe('model-x');
  });
});
