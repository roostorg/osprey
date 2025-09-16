import * as React from 'react';
import { Select } from 'antd';
import shallow from 'zustand/shallow';

import useApplicationConfigStore from '../../stores/ApplicationConfigStore';
import useQueryStore from '../../stores/QueryStore';
import { isEmptyDateRange } from '../../utils/QueryUtils';
import OptionLabelWithInfo from '../common/OptionLabelWithInfo';
import { filterAutoComplete, sortOptions } from '../../utils/AutoCompleteUtils';

const FeatureNameSelect = React.memo(
  ({
    selectedFeatureName,
    onFeatureNameChange,
  }: {
    selectedFeatureName: string;
    onFeatureNameChange: (val: string) => void;
  }): React.ReactElement => {
    const [executedQuery, topNTables] = useQueryStore((state) => [state.executedQuery, state.topNTables], shallow);
    const [knownFeatureNames, ruleInfoMapping] = useApplicationConfigStore((state) => [
      state.knownFeatureNames,
      state.ruleInfoMapping,
    ]);

    const options = [...knownFeatureNames]
      .filter((featureName) => !topNTables.has(featureName))
      .map((option: string) => ({
        value: option,
        label: <OptionLabelWithInfo option={option} optionInfoMapping={ruleInfoMapping} />,
      }));

    const [filteredOptions, setFilteredOptions] = React.useState(options);

    const handleSearch = (inputValue: string) => {
      const sortedOptions = options
        .filter((option) => filterAutoComplete(inputValue, option))
        .sort(sortOptions(inputValue));

      setFilteredOptions(sortedOptions);
    };

    return (
      <Select
        showSearch
        placeholder="Select Feature Name"
        value={selectedFeatureName}
        onChange={onFeatureNameChange}
        onSearch={(inputText) => {
          handleSearch(inputText);
        }}
        style={{ width: '100%' }}
        disabled={isEmptyDateRange(executedQuery.start, executedQuery.end)}
        filterOption={false}
        options={filteredOptions}
      />
    );
  }
);

export default FeatureNameSelect;
