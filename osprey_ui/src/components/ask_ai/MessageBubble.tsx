import * as React from 'react';
import { Spin } from 'antd';

import Markdown from './Markdown';
import { AskMessage } from './types';

import styles from './AskAiPanel.module.css';

const MessageBubble = ({ message }: { message: AskMessage }): React.ReactElement => {
  const isUser = message.role === 'user';
  if (isUser) {
    return (
      <div className={styles.userBubble}>
        <span>{message.text}</span>
      </div>
    );
  }
  return (
    <div className={styles.assistantBubble}>
      {message.text ? <Markdown>{message.text}</Markdown> : null}
      {message.streaming ? <Spin size="small" data-testid="streaming-indicator" /> : null}
      {message.error ? <span className={styles.errorText}>{message.error.message}</span> : null}
    </div>
  );
};

export default MessageBubble;
