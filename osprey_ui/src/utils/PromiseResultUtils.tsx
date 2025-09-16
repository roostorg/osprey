import * as React from 'react';
import { ReloadOutlined } from '@ant-design/icons';
import { Spin, Alert, Button } from 'antd';
import { sample } from 'lodash';

import { PromiseResult, PromiseResultStatus } from '../hooks/usePromiseResult';

interface RenderResolvedPromiseOptions {
  // A custom function that will render a specific "resolving" state, rather than the default loading UI.
  renderResolving?: () => React.ReactElement | null;
  // A custom function that will render a specific "rejected" state, rather than the default rejection UI.
  renderRejected?: (error: Error, retry: () => void) => React.ReactElement;

  showStackTrace?: boolean;
}

const loadingLines = [
  'Upsorbing the Contents',
  'Additive Parsing the Load',
  'Commence Monosaturated Goodening',
  'Kick Off the Multi-Core Widening',
  'Bastening the Game Turkey',
  'Abstracting the Rummage Disc',
  'Undecerealenizing the Process',
  'Postrefragmenting the Widget Layer',
  'Satisfying the Constraints',
  'Abnoramalzing Some of the Matrices',
  'Optimizing the People',
  'Proclaigerizing the Network',
];

function Resolving() {
  const loadingLine = React.useMemo(() => sample(loadingLines), []);
  return (
    <Spin tip={loadingLine} size="large">
      <div style={{ height: '100px' }} />
    </Spin>
  );
}

function Rejected({ error, retry, showStackTrace }: { error: Error; retry: () => void; showStackTrace: boolean }) {
  // Sanitize the stack trace a bit better, by stripping the first line (which will usually be Error: ${error.message})
  const stackTrace =
    showStackTrace && error.stack ? (
      <pre>
        {error.stack
          .split('\n')
          .slice(1)
          .map((l) => l.trim())
          .join('\n')}
      </pre>
    ) : null;

  const description = (
    <>
      {stackTrace}
      <Button icon={<ReloadOutlined />} size="middle" onClick={retry}>
        Retry
      </Button>
    </>
  );

  return <Alert message={<code>{error.message}</code>} description={description} type="error" showIcon />;
}

export function renderFromPromiseResult<T>(
  promiseResult: PromiseResult<T>,
  renderResolved: (resolved: T) => React.ReactElement,
  options: RenderResolvedPromiseOptions = {}
): React.ReactElement | null {
  switch (promiseResult.status) {
    case PromiseResultStatus.Resolving:
      return options.renderResolving?.() ?? <Resolving />;
    case PromiseResultStatus.Resolved:
      return renderResolved(promiseResult.value);
    case PromiseResultStatus.Rejected:
      const { error, retry } = promiseResult;
      return (
        options.renderRejected?.(error, retry) ?? (
          <Rejected error={error} retry={retry} showStackTrace={options.showStackTrace ?? true} />
        )
      );
  }
}
