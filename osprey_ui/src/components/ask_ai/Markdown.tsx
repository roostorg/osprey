import * as React from 'react';
import ReactMarkdown, { type Components } from 'react-markdown';

interface MarkdownProps {
  children: string;
}

// react-markdown does not render raw HTML unless rehype-raw is enabled (it is not), so
// embedded HTML in model output is treated as literal text -- XSS-safe by construction.
const components: Components = {
  a: ({ href, children }) => (
    <a href={href} target="_blank" rel="noopener noreferrer">
      {children}
    </a>
  ),
};

const Markdown = ({ children }: MarkdownProps): React.ReactElement => (
  <ReactMarkdown components={components}>{children}</ReactMarkdown>
);

export default Markdown;
