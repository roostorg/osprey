import * as React from 'react';
import { Drawer } from 'antd';

import { getSavedQueryHistory } from '../../actions/SavedQueryActions';
import usePromiseResult from '../../hooks/usePromiseResult';
import { SavedQuery } from '../../types/QueryTypes';
import Text, { TextSizes } from '../../uikit/Text';
import { renderFromPromiseResult } from '../../utils/PromiseResultUtils';
import SavedQueryCard, { SavedQueryCardTypes } from './SavedQueryCard';

import styles from './SavedQueryDrawer.module.css';

interface SavedQueryDrawerProps {
  savedQuery: SavedQuery | null;
  onClose: () => void;
}

const SavedQueryDrawerContent = ({ savedQuery }: { savedQuery: SavedQuery }) => {
  const savedQueryHistoryResults = usePromiseResult(() => getSavedQueryHistory(savedQuery.id));

  return renderFromPromiseResult(savedQueryHistoryResults, (savedQueryHistory) => {
    const renderQueryHistoryList = () => {
      if (savedQueryHistory.length === 0) {
        return <Text size={TextSizes.H7}>No version history</Text>;
      }

      return savedQueryHistory.map((query) => (
        <SavedQueryCard
          key={query.id}
          savedQuery={savedQuery}
          query={query}
          type={SavedQueryCardTypes.VERSION_HISTORY}
        />
      ));
    };

    return (
      <>
        <Text className={styles.queryDrawerHeader} size={TextSizes.H4}>
          Version History: {savedQuery.name}
        </Text>
        <div>{renderQueryHistoryList()}</div>
      </>
    );
  });
};

const SavedQueryDrawer = ({ savedQuery, onClose }: SavedQueryDrawerProps) => {
  const renderDrawerContent = () => {
    if (savedQuery == null) return null;
    return <SavedQueryDrawerContent savedQuery={savedQuery} />;
  };

  return (
    <Drawer
      placement="right"
      width={750}
      visible={savedQuery != null}
      closable={false}
      onClose={onClose}
      destroyOnClose
    >
      {renderDrawerContent()}
    </Drawer>
  );
};

export default SavedQueryDrawer;
