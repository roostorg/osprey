import * as React from 'react';

import EvidenceList from './EvidenceList';
import MessageBubble from './MessageBubble';
import { AskEvidence, AskMessage } from './types';

interface MessageListProps {
  messages: AskMessage[];
  renderEvidence?: (evidence: AskEvidence) => React.ReactNode;
  evidenceActions?: (evidence: AskEvidence) => React.ReactNode;
}

const MessageList = ({ messages, renderEvidence, evidenceActions }: MessageListProps): React.ReactElement => {
  const endRef = React.useRef<HTMLDivElement>(null);
  React.useEffect(() => {
    endRef.current?.scrollIntoView({ block: 'end' });
  }, [messages]);
  return (
    <div>
      {messages.map((message) => (
        <div key={message.id}>
          <MessageBubble message={message} />
          {message.evidence.length > 0 ? (
            <EvidenceList
              evidence={message.evidence}
              renderEvidence={renderEvidence}
              evidenceActions={evidenceActions}
            />
          ) : null}
        </div>
      ))}
      <div ref={endRef} />
    </div>
  );
};

export default MessageList;
