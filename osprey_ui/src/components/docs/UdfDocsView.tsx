import * as React from 'react';
import classNames from 'classnames';
import hljs from 'highlight.js';

import { getUdfDocs } from '../../actions/DocActions';
import usePromiseResult from '../../hooks/usePromiseResult';
import { UdfArgumentSpec, UdfCategory, UdfMethodSpec } from '../../types/DocTypes';
import Text, { TextSizes } from '../../uikit/Text';
import { renderFromPromiseResult } from '../../utils/PromiseResultUtils';

import styles from './UdfDocsView.module.css';

/*
TODO:
- Set up page for other non-UDF documentation (eg have UDF header, probably change URL to be just `/docs`)
- Ability to link to individual function's position on page, have hover button on each UDF title that navigates to
    the UDF's URL
- TOC at side
- Render markdown (or at least simple subsets?) in the docs
 */

interface UdfDocProps {
  udf: UdfMethodSpec;
}

const UdfDoc = ({ udf }: UdfDocProps) => {
  const argumentAsParam = (arg: UdfArgumentSpec): string => {
    const defaultPart = arg.default != null ? ` = ${arg.default}` : '';
    return `${arg.name}: ${arg.type}${defaultPart}`;
  };

  const params = udf.argument_specs.map(argumentAsParam).join(', ');
  const hasAnyArgDocs = udf.argument_specs.some((spec) => spec.doc != null);

  const mainDoc = udf.doc != null && <Text size={TextSizes.LARGE}>{udf.doc}</Text>;
  const argDocs = udf.argument_specs.map(
    (spec) =>
      spec.doc != null && (
        <Text size={TextSizes.LARGE} key={spec.name}>
          <code className={styles.udfParamName}>{spec.name}</code>: {spec.doc}
        </Text>
      )
  );
  const argDocsTitle = hasAnyArgDocs && <Text size={TextSizes.H5}>Parameters</Text>;
  const noDocsNotice = udf.doc == null && !hasAnyArgDocs && <Text size={TextSizes.LARGE}>No documentation yet!</Text>;

  // We need to add the `def` prefix and `: ...` suffix to ensure highlight.js recognizes this as a function
  // declaration, but we don't actually want to display them, so we remove them from the processed HTML.
  const methodSignature = hljs.highlight('python', `def ${udf.name}(${params}) -> ${udf.return_type}: ...`, true);
  const methodSignatureRawHtml = methodSignature.value
    .replace('<span class="hljs-keyword">def</span> ', '')
    .replace(':</span> ...', '</span>');

  return (
    <>
      <Text size={TextSizes.LARGE}>
        <pre className={styles.methodSignatureDisplayArea}>
          <code
            className={classNames(styles.methodSignatureHighlightedWrap, 'python')}
            dangerouslySetInnerHTML={{ __html: methodSignatureRawHtml }}
          />
        </pre>
      </Text>
      <div className={styles.udfDetails}>
        {mainDoc}
        {argDocsTitle}
        {argDocs}
        {noDocsNotice}
      </div>
    </>
  );
};

const renderUdfCategory = (category: UdfCategory) => {
  const categoryName = category.name || 'Other';
  return (
    <div className={styles.udfCategoryContainer} id={categoryName}>
      <Text size={TextSizes.H4} className={styles.udfCategoryName} key={`category-${categoryName}`}>
        {categoryName}
      </Text>
      {category.udfs.map((udf: UdfMethodSpec) => (
        <UdfDoc key={udf.name} udf={udf} />
      ))}
    </div>
  );
};

const UdfDocsView = () => {
  const udfDocsResult = usePromiseResult(getUdfDocs);

  return renderFromPromiseResult(udfDocsResult, (udfDocs) => (
    <div className={styles.viewContainer}>
      <div className={styles.udfDocsContainer}>{udfDocs.map(renderUdfCategory)}</div>
    </div>
  ));
};

export default UdfDocsView;
