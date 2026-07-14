import * as React from 'react';
import { Card, Tag } from 'antd';

import { AskEvidence } from './types';

import styles from './AskAiPanel.module.css';

interface EvidenceListProps {
  evidence: AskEvidence[];
  renderEvidence?: (evidence: AskEvidence) => React.ReactNode;
  evidenceActions?: (evidence: AskEvidence) => React.ReactNode;
}

const EvidenceList = ({ evidence, renderEvidence, evidenceActions }: EvidenceListProps): React.ReactElement => (
  <div className={styles.evidence}>
    {evidence.map((item, index) => (
      <div key={item.toolCallId ?? index} className={styles.evidenceItem}>
        {renderEvidence ? (
          renderEvidence(item)
        ) : (
          <Card
            size="small"
            title={item.name ?? 'Query'}
            extra={item.isError ? <Tag color="red">error</Tag> : <Tag color="green">ok</Tag>}
          >
            <pre className={styles.evidenceContent}>{item.content}</pre>
          </Card>
        )}
        {evidenceActions ? <div className={styles.evidenceActions}>{evidenceActions(item)}</div> : null}
      </div>
    ))}
  </div>
);

export default EvidenceList;
