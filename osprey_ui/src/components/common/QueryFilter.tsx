import * as React from 'react';
import classNames from 'classnames';
import hljs from 'highlight.js';

import 'highlight.js/styles/atelier-cave-light.css';
import { querySearchContext } from '../query_view/QuerySearchContext';
import { splitStringIncludingMatches, highlightMatchedText } from '../../utils/StringUtils';

interface QueryFilterProps {
  queryFilter: string;
  className?: string;
  codeClassName?: string;
  ref?: React.RefObject<HTMLPreElement>;
}

const QueryFilter = React.forwardRef(
  ({ queryFilter, className, codeClassName }: QueryFilterProps, ref: React.Ref<HTMLPreElement>) => {
    const codeRef = React.useRef<HTMLElement>(null);

    const searchQuery = React.useContext(querySearchContext);

    const queryTokens = splitStringIncludingMatches(queryFilter, searchQuery.query, searchQuery.regex);
    const highlightSearchQuery = queryTokens.find((el) => el.matched) !== undefined;

    React.useEffect(() => {
      const codeElement = codeRef.current;
      if (highlightSearchQuery) {
        // skip syntax highlighting if highlighting a search query
        return;
      }
      if (codeElement) hljs.highlightBlock(codeElement);
    }, [searchQuery, codeRef, queryFilter]);

    return (
      <pre ref={ref} className={className}>
        {highlightSearchQuery ? (
          <code className={classNames(codeClassName, 'python')} ref={codeRef}>
            {highlightMatchedText(queryTokens)}
          </code>
        ) : (
          <code
            className={classNames(codeClassName, 'python')}
            dangerouslySetInnerHTML={{ __html: hljs.highlight('python', queryFilter, true).value }}
          />
        )}
      </pre>
    );
  }
);
export default QueryFilter;
