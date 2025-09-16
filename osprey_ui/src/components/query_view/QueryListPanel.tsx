import * as React from 'react';
import { Input, Menu, Select } from 'antd';

import useApplicationConfigStore from '../../stores/ApplicationConfigStore';
import QueryHistoryList from '../query_history/QueryHistoryList';
import { QueryRecordCardTypes } from '../query_history/QueryRecordCard';
import { SavedQueryCardTypes } from '../saved_queries/SavedQueryCard';
import SavedQueryList from '../saved_queries/SavedQueryList';

import { querySearchContext } from './QuerySearchContext';

import styles from './QueryListPanel.module.css';

enum QueryListPanelTabs {
  HISTORY = 'HISTORY',
  SAVED_QUERIES = 'SAVED_QUERIES',
}

enum QueryListFilterOptions {
  ALL = 'ALL',
  MINE = 'MINE',
}

const QueryListPanel = () => {
  const currentUser = useApplicationConfigStore((state) => state.currentUser);
  const [selectedTab, setSelectedTab] = React.useState(QueryListPanelTabs.HISTORY);
  const [filterOption, setFilterOption] = React.useState(QueryListFilterOptions.ALL);
  const [search, setSearch] = React.useState('');
  const [regexEnabled, setRegexEnabled] = React.useState(false);
  const handleMenuItemClick = (e: { key: React.Key }) => {
    setSelectedTab(e.key as QueryListPanelTabs);
  };

  const renderQueryList = () => {
    let user;

    if (filterOption === QueryListFilterOptions.MINE) {
      user = currentUser;
    }

    switch (selectedTab) {
      case QueryListPanelTabs.HISTORY:
        return <QueryHistoryList userEmail={user?.email} cardType={QueryRecordCardTypes.SIDEBAR} />;
      case QueryListPanelTabs.SAVED_QUERIES:
        return (
          <SavedQueryList
            userEmail={user?.email}
            cardType={search.length ? SavedQueryCardTypes.DETAILED : SavedQueryCardTypes.COMPACT}
          />
        );
    }
  };

  return (
    <>
      <RegexSearchInput
        query={search}
        setQuery={setSearch}
        regexEnabled={regexEnabled}
        setRegexEnabled={setRegexEnabled}
      />
      <querySearchContext.Provider value={{ query: search, regex: regexEnabled }}>
        <div className={styles.menuWrapper}>
          <Menu onClick={handleMenuItemClick} mode="horizontal" selectedKeys={[selectedTab]}>
            <Menu.Item key={QueryListPanelTabs.HISTORY}>History</Menu.Item>
            <Menu.Item key={QueryListPanelTabs.SAVED_QUERIES}>Saved Queries</Menu.Item>
          </Menu>
          <Select onChange={setFilterOption} className={styles.filterSelect} defaultValue={QueryListFilterOptions.ALL}>
            <Select.Option value={QueryListFilterOptions.ALL}>All</Select.Option>
            <Select.Option value={QueryListFilterOptions.MINE}>My Queries</Select.Option>
          </Select>
        </div>
        {renderQueryList()}
      </querySearchContext.Provider>
    </>
  );
};

export default QueryListPanel;

const RegexToggleButton = ({
  regexEnabled,
  setRegexEnabled,
}: {
  regexEnabled: boolean;
  setRegexEnabled: (regex: boolean) => void;
}) => {
  return (
    <button
      onClick={() => setRegexEnabled(!regexEnabled)}
      title={regexEnabled ? 'Disable RegEx' : 'Enable RegEx'}
      data-enabled={regexEnabled}
      className={styles.regexButton}
    >
      .*
    </button>
  );
};
type RegexSearchInputProps = {
  query: string;
  setQuery: (query: string) => void;
  regexEnabled: boolean;
  setRegexEnabled: (regex: boolean) => void;
};
const RegexSearchInput = ({ query, regexEnabled, setQuery, setRegexEnabled }: RegexSearchInputProps) => {
  return (
    <Input.Search
      value={query}
      onInput={(e) => setQuery(e.currentTarget.value)}
      prefix={<span style={{ color: '#00000050', width: '1ch' }}>{regexEnabled ? '/' : ''}</span>}
      suffix={
        <>
          <span style={{ color: '#00000050', width: '2ch' }}>{regexEnabled ? '/g' : ''}</span>
          <RegexToggleButton regexEnabled={regexEnabled} setRegexEnabled={setRegexEnabled} />
        </>
      }
    />
  );
};
