import * as React from 'react';
import classNames from 'classnames';

import OspreyButton, { ButtonColors } from './OspreyButton';
import Text, { TextSizes, TextWeights } from './Text';

import { Colors } from '../Constants';
import styles from './Expand.module.css';

interface ExpandContextT {
  isExpandable: boolean;
  isExpanded: boolean;
  setIsExpandable: (isExpandable: boolean) => void;
  setIsExpanded: (isExpanded?: boolean) => void;
  rowsNotShown: number;
  setRowsNotShown: (rows: number) => void;
}

const ExpandContext = React.createContext<ExpandContextT>({
  isExpandable: false,
  isExpanded: false,
  setIsExpandable: (_isExpandable: boolean) => {},
  setIsExpanded: () => {},
  rowsNotShown: 0,
  setRowsNotShown: (_rows: number) => {},
});

export const ExpandContextProvider = ({ children }: { children: React.ReactNode }) => {
  const [isExpandable, setIsExpandable] = React.useState(false);
  const [isExpanded, setIsExpanded] = React.useState(false);
  const [rowsNotShown, setRowsNotShown] = React.useState(0);

  const handleSetCanExpand = (newCanExpand: boolean) => {
    setIsExpandable(newCanExpand);
  };

  const handleSetIsExpanded = () => {
    setIsExpanded(!isExpanded);
  };

  const handleSetRowsNotShown = (rows: number) => {
    setRowsNotShown(rows);
  };

  return (
    <ExpandContext.Provider
      value={{
        isExpandable,
        isExpanded,
        rowsNotShown,
        setIsExpandable: handleSetCanExpand,
        setIsExpanded: handleSetIsExpanded,
        setRowsNotShown: handleSetRowsNotShown,
      }}
    >
      {children}
    </ExpandContext.Provider>
  );
};

export const ExpandButton = () => {
  const { isExpandable, setIsExpanded, rowsNotShown } = React.useContext(ExpandContext);

  return (
    <>
      {isExpandable ? (
        <OspreyButton className={styles.expandButton} color={ButtonColors.LINK_GRAY} onClick={() => setIsExpanded()}>
          <Text
            size={TextSizes.SMALL}
            weight={TextWeights.SEMIBOLD}
            className={styles.remainingRowsPill}
          >{`+${rowsNotShown} Lines`}</Text>
        </OspreyButton>
      ) : null}
    </>
  );
};

interface ExpandProps {
  children: React.ReactNode;
  className?: string;
  rowHeight?: number;
  fadeColor?: string;
}

const Expand = ({ children, className, rowHeight, fadeColor = Colors.BACKGROUND_SECONDARY_ALT }: ExpandProps) => {
  const { isExpanded, setIsExpandable, isExpandable, setRowsNotShown } = React.useContext(ExpandContext);
  const childNodeHeight: React.MutableRefObject<number | null> = React.useRef(null);

  const childNodeRef = React.useCallback(
    (node: HTMLDivElement | null) => {
      if (node != null) {
        const childNode = node.getBoundingClientRect();
        const parentNode = (node.parentNode as HTMLElement).getBoundingClientRect();
        if (childNode.height > parentNode.height) {
          childNodeHeight.current = childNode.height;
          setIsExpandable(true);

          if (rowHeight != null) {
            const remainingHeight = childNode.height - parentNode.height;
            setRowsNotShown(Math.round(remainingHeight) / rowHeight);
          }
        }
      }
    },
    [rowHeight, setIsExpandable, setRowsNotShown]
  );

  const renderChildNode = () => {
    if (children == null) return null;
    return React.cloneElement(children as React.ReactElement<any>, { ref: childNodeRef });
  };

  const renderGradientOverlay = () => {
    if (!isExpandable) return null;
    return (
      <div
        style={{ backgroundImage: `linear-gradient(to bottom, transparent, ${fadeColor}` }}
        className={classNames(styles.expandable, { [styles.expanded]: isExpanded })}
      />
    );
  };

  const getParentDivStyle = (): React.CSSProperties => {
    if (isExpanded && childNodeHeight.current != null) {
      return { maxHeight: childNodeHeight.current };
    }

    return {};
  };

  /* eslint-disable */
  return (
    <div className={classNames(styles.expandContainer, className)} style={getParentDivStyle()}>
      {renderChildNode()}
      {renderGradientOverlay()}
    </div>
  );
  /* eslint-enable */
};

export default Expand;
