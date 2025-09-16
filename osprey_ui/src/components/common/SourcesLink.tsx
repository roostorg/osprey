import * as React from 'react';
import { GithubOutlined } from '@ant-design/icons';

import { parsePathString, getSourcesUrl } from '../../utils/SourceUtils';

interface SourcesLinkProps {
  path: string;
}
export default function SourcesLink({ path }: SourcesLinkProps) {
  const pathInfo = parsePathString(path);
  if (pathInfo == null) {
    // eslint-disable-next-line no-console
    console.warn(`Tried to render SourcesLink for invalid path: ${path}`);
    return <>{path}</>;
  }

  return (
    <>
      {/* eslint-disable-next-line */}
      <a href={getSourcesUrl(pathInfo)} target="_blank" rel="noopener noreferrer">
        <GithubOutlined /> {pathInfo.path}:{pathInfo.lineNumber}
      </a>
      {pathInfo.rest}
    </>
  );
}
