import * as React from 'react';
import { Checkbox, Input, Modal, Switch } from 'antd';
import { CheckboxChangeEvent } from 'antd/lib/checkbox/Checkbox';
import classNames from 'classnames';

import useApplicationConfigStore from '../../stores/ApplicationConfigStore';
import useQueryStore from '../../stores/QueryStore';
import OspreyButton from '../../uikit/OspreyButton';
import Text, { TextSizes, TextWeights, TextColors } from '../../uikit/Text';
import FilterIcon from '../../uikit/icons/FilterIcon';
import ModalFooter from '../common/ModalFooter';
import CustomFeaturesFilter from './CustomFeaturesFilter';

import styles from './FeatureSelectModal.module.css';

const FeatureSelectModal = () => {
  const customSummaryFeatures = useQueryStore((state) => state.customSummaryFeatures);
  const updateCustomSummaryFeatures = useQueryStore((state) => state.updateCustomSummaryFeatures);
  const knownFeatureCategories = useApplicationConfigStore((state) => state.knownFeatureCategories);

  const [useCustomFeatures, setUseCustomFeatures] = React.useState(customSummaryFeatures != null);
  const [selectedFeatures, setSelectedFeatures] = React.useState<Set<string>>(
    customSummaryFeatures != null ? new Set(customSummaryFeatures) : new Set()
  );

  const [isOpen, setIsOpen] = React.useState(false);
  const [searchInput, setSearchInput] = React.useState('');

  const sortedKnownFeatureCategoryEntries = React.useMemo(
    () => Object.entries(knownFeatureCategories).sort(),
    [knownFeatureCategories]
  );

  const handleCancel = () => {
    setUseCustomFeatures(customSummaryFeatures != null);
    setSelectedFeatures(customSummaryFeatures != null ? new Set(customSummaryFeatures) : new Set());
    setIsOpen(false);
  };

  const handleOpenModal = () => {
    setIsOpen(true);
  };

  const handleSelectFeature = (e: CheckboxChangeEvent) => {
    const selectedFeature = e.target.value;
    const updatedFeatures = new Set([...selectedFeatures]);

    if (selectedFeatures.has(selectedFeature)) {
      updatedFeatures.delete(selectedFeature);
    } else {
      updatedFeatures.add(selectedFeature);
    }
    setSelectedFeatures(updatedFeatures);
  };

  const handleSaveSelectedFeatures = () => {
    const saveVal = useCustomFeatures ? [...selectedFeatures] : null;

    updateCustomSummaryFeatures(saveVal);
    setIsOpen(false);
  };

  const handleToggleSwitch = () => {
    if (useCustomFeatures) {
      setSelectedFeatures(new Set());
    }

    setUseCustomFeatures(!useCustomFeatures);
  };

  const handleSearchInputChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    setSearchInput(e.currentTarget.value);
  };

  const getReadableFeatureCategory = (featureCategory: string) => {
    return featureCategory.substring(0, featureCategory.lastIndexOf('.')).replace(/_|\//g, ' ');
  };

  const renderModalTitle = () => {
    return (
      <>
        <Text size={TextSizes.H4}>Set Features</Text>
        <span className={styles.switchSpan}>
          <Switch
            onChange={handleToggleSwitch}
            className={styles.switch}
            defaultChecked={false}
            checked={useCustomFeatures}
          />
          <Text size={TextSizes.LARGE} weight={TextWeights.NORMAL} color={TextColors.LIGHT_PRIMARY}>
            Set Custom Features
          </Text>
        </span>
        <CustomFeaturesFilter
          selectedCustomFeatures={selectedFeatures}
          useCustomFeatures={useCustomFeatures}
          onClearFilters={() => setSelectedFeatures(new Set())}
          onRemoveSelectedFeature={setSelectedFeatures}
        />
        <Input
          allowClear
          className={styles.__invalid_search}
          disabled={!useCustomFeatures}
          placeholder="Filter feature list"
          onChange={handleSearchInputChange}
          addonBefore={<FilterIcon width={12} height={12} />}
        />
      </>
    );
  };

  const renderFeatureCategory = (category: string, features: string[]) => {
    const filteredFeatures = features.filter((feature) => feature.toUpperCase().includes(searchInput.toUpperCase()));
    if (filteredFeatures.length === 0) return null;

    return (
      <div key={category} className={styles.innerCheckboxGrid}>
        <Text
          size={TextSizes.H5}
          className={classNames(styles.category, {
            [styles.disabled]: !useCustomFeatures,
          })}
        >
          {getReadableFeatureCategory(category)}
        </Text>
        {filteredFeatures.sort().map((feature) => {
          return (
            <Checkbox
              key={feature as string}
              value={feature}
              disabled={!useCustomFeatures}
              onChange={handleSelectFeature}
              checked={selectedFeatures.has(feature as string)}
              className={styles.featureCheckboxRow}
            >
              <span className={styles.featureSpan}>{feature}</span>
            </Checkbox>
          );
        })}
      </div>
    );
  };

  return (
    <>
      <OspreyButton onClick={handleOpenModal}>Select Summary Features</OspreyButton>
      <Modal
        title={renderModalTitle()}
        className={styles.featureModal}
        width={526}
        visible={isOpen}
        onCancel={handleCancel}
        footer={<ModalFooter onOK={handleSaveSelectedFeatures} onCancel={handleCancel} />}
      >
        <div className={styles.checkboxGrid}>
          {sortedKnownFeatureCategoryEntries.map(([category, features]) => {
            return renderFeatureCategory(category, features);
          })}
        </div>
      </Modal>
    </>
  );
};

export default FeatureSelectModal;
