import * as React from 'react';
import { Button, Input, Select, Spin } from 'antd';

import { createAskStore } from './AskStore';
import { AskAiPanelProps } from './AskAiPanel.types';
import MessageList from './MessageList';

import styles from './AskAiPanel.module.css';

const { TextArea } = Input;

const AskAiPanel = (props: AskAiPanelProps): React.ReactElement => {
  const {
    endpoint,
    models,
    defaultModel,
    getContextSnapshot,
    renderEvidence,
    evidenceActions,
    placeholder,
    title,
    className,
  } = props;

  // One isolated store per mounted panel; the factory is a stable test/advanced seam.
  const useStore = React.useMemo(
    () => (props.createStore ?? createAskStore)(),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    []
  );
  const [input, setInput] = React.useState('');

  const status = useStore((s) => s.status);
  const messages = useStore((s) => s.messages);
  const model = useStore((s) => s.model);
  const configure = useStore((s) => s.configure);
  const sendMessage = useStore((s) => s.sendMessage);
  const retry = useStore((s) => s.retry);
  const newChat = useStore((s) => s.newChat);
  const abort = useStore((s) => s.abort);

  React.useEffect(() => {
    configure(endpoint, defaultModel);
  }, [configure, endpoint, defaultModel]);

  const streaming = status === 'streaming';

  const handleSend = React.useCallback(async (): Promise<void> => {
    const text = input.trim();
    if (!text || streaming) {
      return;
    }
    setInput('');
    const contextRef = getContextSnapshot ? await getContextSnapshot() : undefined;
    await sendMessage(text, contextRef ? { contextRef } : undefined);
  }, [input, streaming, getContextSnapshot, sendMessage]);

  const handleKeyDown = (event: React.KeyboardEvent<HTMLTextAreaElement>): void => {
    if (event.key === 'Enter' && !event.shiftKey) {
      event.preventDefault();
      void handleSend();
    }
  };

  return (
    <section className={[styles.panel, className].filter(Boolean).join(' ')} aria-label={title ?? 'Ask AI'}>
      <header className={styles.header}>
        <span className={styles.title}>{title ?? 'Ask AI'}</span>
        <div className={styles.controls}>
          {models && models.length > 0 ? (
            <Select
              aria-label="Model"
              size="small"
              value={model}
              options={models}
              onChange={(value: string) => configure(endpoint, value)}
              className={styles.modelSelect}
            />
          ) : null}
          <Button size="small" onClick={newChat} disabled={streaming}>
            New chat
          </Button>
        </div>
      </header>

      <div className={styles.messages} role="log" aria-live="polite">
        <MessageList messages={messages} renderEvidence={renderEvidence} evidenceActions={evidenceActions} />
      </div>

      {status === 'error' ? (
        <div className={styles.retryRow}>
          <Button size="small" onClick={() => void retry()}>
            Retry
          </Button>
        </div>
      ) : null}

      <footer className={styles.composer}>
        <TextArea
          aria-label="Message"
          value={input}
          placeholder={placeholder ?? 'Ask a question…'}
          onChange={(event) => setInput(event.target.value)}
          onKeyDown={handleKeyDown}
          autoSize={{ minRows: 1, maxRows: 6 }}
          disabled={streaming}
        />
        {streaming ? (
          <Button onClick={abort}>
            <Spin size="small" /> Stop
          </Button>
        ) : (
          <Button type="primary" onClick={() => void handleSend()} disabled={input.trim().length === 0}>
            Send
          </Button>
        )}
      </footer>
    </section>
  );
};

export default AskAiPanel;
