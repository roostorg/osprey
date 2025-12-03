import * as React from 'react';
import { AutoComplete, Input, Select } from 'antd';

import { SelectValue } from 'antd/lib/select';
import { OptionData } from 'rc-select/lib/interface/index';

import shallow from 'zustand/shallow';

import { validateQuery } from '../../actions/QueryActions';
import useApplicationConfigStore from '../../stores/ApplicationConfigStore';
import useErrorStore from '../../stores/ErrorStore';
import useQueryStore from '../../stores/QueryStore';
import { IntervalOptions, DefaultIntervals } from '../../types/QueryTypes';
import OspreyButton, { ButtonColors } from '../../uikit/OspreyButton';
import { TextSizes } from '../../uikit/Text';
import QueryIcon from '../../uikit/icons/QueryIcon';
import { filterAutoComplete, sortOptions } from '../../utils/AutoCompleteUtils';
import { getQueryDateRange, isEmptyDateRange, CUSTOM_RANGE_OPTION } from '../../utils/QueryUtils';
import IconHeader from '../common/IconHeader';
import QueryFilter from '../common/QueryFilter';
import OptionLabelWithInfo from '../common/OptionLabelWithInfo';
import QueryErrors from './QueryErrors';

import styles from './QueryInput.module.css';

const SelectOptions = [
  ...Object.entries(IntervalOptions).map(([option, { label }]) => ({
    value: option,
    label: label,
  })),
  { value: CUSTOM_RANGE_OPTION, label: 'Custom Range' },
];

export interface QueryInputProps {
  interval: DefaultIntervals;
  dateRange: { start: string; end: string };
  onIntervalChange: (interval: DefaultIntervals) => void;
}

const QueryInput = ({
  interval: activeQueryInterval,
  onIntervalChange: onActiveQueryIntervalChange,
  dateRange,
}: QueryInputProps) => {
  const [executedQuery, updateExecutedQuery] = useQueryStore(
    (state) => [state.executedQuery, state.updateExecutedQuery],
    shallow
  );
  const [knownFeatureNames, knownActionNames, ruleInfoMapping] = useApplicationConfigStore(
    (state) => [state.knownFeatureNames, state.knownActionNames, state.ruleInfoMapping],
    shallow
  );
  const [queryFilter, setQueryFilter] = React.useState(executedQuery.queryFilter);
  const [searchOptions, setSearchOptions] = React.useState<OptionData[]>([]);
  const [filteredOptions, setFilteredOptions] = React.useState<OptionData[]>([]);

  // store the selected interval state separate from the current query's interval, so that changes
  // to the interval can be made without losing WIP in the query input field
  const [selectedInterval, setSelectedInterval] = React.useState(activeQueryInterval);

  const [isLoading, setIsLoading] = React.useState(false);

  const handleSelectChange = (value: DefaultIntervals) => {
    // only update the active query's interval if the user has not made any changes to their query input
    if (executedQuery.queryFilter === queryFilter) {
      onActiveQueryIntervalChange(value);
    }
    setSelectedInterval(value);
  };

  React.useEffect(() => {
    const options = [...knownFeatureNames, ...knownActionNames].map((option: string) => ({
      value: option,
      label: <OptionLabelWithInfo option={option} optionInfoMapping={ruleInfoMapping} />,
    }));
    setSearchOptions(options);
    setFilteredOptions(options);
  }, [knownFeatureNames, knownActionNames]);

  React.useEffect(() => {
    setQueryFilter(executedQuery.queryFilter);
    // if the user has selected a new query to execute from the saved queries list, make sure that
    // we update the selected interval to reflect the correct one
    if (executedQuery.interval !== selectedInterval) {
      setSelectedInterval(executedQuery.interval);
    }
  }, [executedQuery]);

  const handleSelectAutoComplete = (value: SelectValue) => {
    let insertValue = String(value);

    if (knownActionNames.has(insertValue)) {
      insertValue = `"${insertValue}"`;
    }

    const textAreaValueArr = queryFilter.split(' ');
    textAreaValueArr[textAreaValueArr.length - 1] = insertValue;
    const newTextAreaValue = textAreaValueArr.join(' ');

    setQueryFilter(newTextAreaValue);
  };

  const clearErrors = useErrorStore((state) => state.clearErrors);

  const handleSubmitQuery = async () => {
    setIsLoading(true);
    clearErrors();
    const isValid = queryFilter === '' ? true : await validateQuery(queryFilter);

    if (isValid) {
      const { start, end } = getQueryDateRange(selectedInterval, dateRange);
      updateExecutedQuery({ start, end, queryFilter, interval: selectedInterval });
    }

    setIsLoading(false);
  };

  const handleKeyPress = (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    const ctrlOrMetaKey = e.ctrlKey || e.metaKey;
    if (ctrlOrMetaKey && e.key === 'Enter') {
      handleSubmitQuery();
    }
  };

  const handleTextAreaChange = (e: React.ChangeEvent<HTMLTextAreaElement>) => {
    setQueryFilter(e.target.value);
    handleSearch(e.target.value);
  };

  const handleSearch = (inputValue: string) => {
    const currentWord = String(inputValue.split(' ').slice(-1));
    const filtered = searchOptions
      .filter((option) => filterAutoComplete(inputValue, option))
      .sort(sortOptions(currentWord));

    setFilteredOptions(filtered);
  };

  return (
    <div className={styles.queryInput}>
      <IconHeader size={TextSizes.H4} icon={<QueryIcon />} title="Query" />
      <div className={styles.textareaWrapper}>
        <AutoComplete
          className={styles.autoComplete}
          value={queryFilter}
          options={filteredOptions}
          onSelect={handleSelectAutoComplete}
        >
          <Input.TextArea
            className={styles.__invalid_textArea}
            onChange={handleTextAreaChange}
            onPressEnter={handleKeyPress}
            autoSize={{ minRows: 2 }}
            spellCheck="false"
          />
        </AutoComplete>
        <div className={styles.overlay}>
          <QueryFilter
            queryFilter={queryFilter}
            className={styles.highlightedWrap}
            codeClassName={styles.displayArea}
          />
        </div>
      </div>
      <div className={styles.intervalSelect}>
        <Select
          onChange={handleSelectChange}
          placeholder="Select date range"
          style={{ width: 180 }}
          value={selectedInterval ?? undefined}
        >
          {SelectOptions.map((option) => (
            <Select.Option value={option.value} key={option.value}>
              {option.label}
            </Select.Option>
          ))}
        </Select>
      </div>
      <OspreyButton
        disabled={selectedInterval === CUSTOM_RANGE_OPTION && isEmptyDateRange(dateRange.start, dateRange.end)}
        color={ButtonColors.DARK_BLUE}
        className={styles.submitButton}
        onClick={handleSubmitQuery}
        loading={isLoading}
      >
        Submit Query
      </OspreyButton>
      <QueryErrors />
    </div>
  );
};

export default QueryInput;
