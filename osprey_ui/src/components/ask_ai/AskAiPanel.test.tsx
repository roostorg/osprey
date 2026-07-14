import { afterEach, describe, expect, it } from '@rstest/core';
import { cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react';
import * as React from 'react';

import AskAiPanel from './AskAiPanel';
import { AskAiPanelProps } from './AskAiPanel.types';
import { AskDeps, createAskStore } from './AskStore';
import { AskStreamError } from './sseClient';
import { AskEvent, AskEvidence, AskRequest } from './types';

afterEach(cleanup);

type StreamFn = AskDeps['stream'];

function ev(type: AskEvent['type'], payload: Record<string, unknown> = {}, extra: Partial<AskEvent> = {}): AskEvent {
  return { version: 1, type, payload, ...extra };
}

function streamOf(events: AskEvent[]): StreamFn {
  return async function* () {
    for (const event of events) {
      yield event;
    }
  } as unknown as StreamFn;
}

function capturingStream(events: AskEvent[], calls: AskRequest[]): StreamFn {
  return ((_endpoint: unknown, req: AskRequest) => {
    calls.push(req);
    return (async function* () {
      for (const event of events) {
        yield event;
      }
    })();
  }) as unknown as StreamFn;
}

function renderPanel(overrides: Partial<AskAiPanelProps>, stream: StreamFn): void {
  const props: AskAiPanelProps = {
    endpoint: { url: 'http://test/ask' },
    createStore: () => createAskStore({ stream }),
    ...overrides,
  };
  render(<AskAiPanel {...props} />);
}

function typeAndSend(text: string): void {
  fireEvent.change(screen.getByLabelText('Message'), { target: { value: text } });
  fireEvent.click(screen.getByText('Send'));
}

describe('AskAiPanel', () => {
  it('sends the message with the selected model and context_ref, and renders the answer', async () => {
    const calls: AskRequest[] = [];
    const stream = capturingStream([ev('assistant_message', { text: 'the answer' }), ev('done')], calls);
    renderPanel(
      { defaultModel: 'm1', getContextSnapshot: () => 'ctx-123', createStore: () => createAskStore({ stream }) },
      stream
    );
    typeAndSend('hello');
    await screen.findByText('the answer');
    expect(calls).toHaveLength(1);
    expect(calls[0].message).toBe('hello');
    expect(calls[0].model).toBe('m1');
    expect(calls[0].context_ref).toBe('ctx-123');
  });

  it('renders query evidence with the default read-only card', async () => {
    const stream = streamOf([
      ev('query_result', { tool_call_id: 't', name: 'lookup', is_error: false, content: 'the-result' }),
      ev('assistant_message', { text: 'done text' }),
      ev('done'),
    ]);
    renderPanel({}, stream);
    typeAndSend('q');
    await screen.findByText('done text');
    expect(screen.getByText('lookup')).toBeTruthy();
    expect(screen.getByText('the-result')).toBeTruthy();
  });

  it('uses a custom evidence renderer when provided', async () => {
    const stream = streamOf([
      ev('query_result', { name: 'x', content: 'c' }),
      ev('assistant_message', { text: 'a' }),
      ev('done'),
    ]);
    const renderEvidence = (e: AskEvidence): React.ReactNode => <div>custom-evidence-{e.name}</div>;
    renderPanel({ renderEvidence }, stream);
    typeAndSend('q');
    await screen.findByText('custom-evidence-x');
    expect(screen.queryByText('Query')).toBeNull();
  });

  it('shows Retry after an error and re-sends the turn', async () => {
    let attempt = 0;
    const stream: StreamFn = (() => {
      attempt += 1;
      if (attempt === 1) {
        return (async function* () {
          yield ev('conversation_started');
          throw new AskStreamError('provider_error', 'boom');
        })();
      }
      return (async function* () {
        yield ev('assistant_message', { text: 'second answer' });
        yield ev('done');
      })();
    }) as unknown as StreamFn;
    renderPanel({}, stream);
    typeAndSend('q');
    const retryButton = await screen.findByText('Retry');
    fireEvent.click(retryButton);
    await screen.findByText('second answer');
  });

  it('shows a safe error message and never a raw exception', async () => {
    const stream: StreamFn = (() =>
      (async function* () {
        yield ev('conversation_started');
        throw new Error('secret stack qqq');
      })()) as unknown as StreamFn;
    renderPanel({}, stream);
    typeAndSend('q');
    await screen.findByText('Something went wrong.');
    expect(screen.queryByText(/qqq/)).toBeNull();
  });

  it('renders assistant markdown formatting without injecting raw HTML', async () => {
    const stream = streamOf([ev('assistant_message', { text: '**strong-word** <img src=x> tail' }), ev('done')]);
    renderPanel({}, stream);
    typeAndSend('q');
    await screen.findByText('strong-word');
    expect(screen.getByText('strong-word').tagName.toLowerCase()).toBe('strong');
    expect(document.body.querySelector('img')).toBeNull();
  });

  it('New chat clears the conversation', async () => {
    const stream = streamOf([ev('assistant_message', { text: 'hi there' }), ev('done')]);
    renderPanel({}, stream);
    typeAndSend('q');
    await screen.findByText('hi there');
    fireEvent.click(screen.getByText('New chat'));
    await waitFor(() => expect(screen.queryByText('hi there')).toBeNull());
  });
});
