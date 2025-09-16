import * as React from 'react';
import classNames from 'classnames';
import shallow from 'zustand/shallow';

import useQueryStore from '../../stores/QueryStore';
import OspreyButton, { ButtonColors, ButtonHeights, ButtonWeights } from '../../uikit/OspreyButton';
import Text, { TextColors, TextSizes, TextWeights } from '../../uikit/Text';
import { addFeaturesToQueryFilter } from '../../utils/QueryUtils';
import { openNewQueryWindow } from '../../utils/SearchParamUtils';

import styles from './Feature.module.css';

interface CommonFeatureProps {
  featureName: string;
  onClick?: () => void;
  className?: string;
  isEntity?: boolean;
  textSelectable?: boolean;
  isFeatureNameOnly?: boolean;
}

type SelectableFeatureProps = CommonFeatureProps & {
  value: string | null;
};

const SelectableFeature = ({
  featureName,
  value,
  onClick,
  className,
  isEntity = false,
  textSelectable = true,
  isFeatureNameOnly = false,
}: SelectableFeatureProps): React.ReactElement => {
  const [queuedQueryFilters, updateQueuedQueryFilters] = useQueryStore(
    (state) => [state.queuedQueryFilters, state.updateQueuedQueryFilters],
    shallow
  );
  const [isSelected, setIsSelected] = React.useState(false);

  React.useEffect(() => {
    if (isSelected && queuedQueryFilters.length === 0) {
      setIsSelected(false);
    }
  }, [isSelected, queuedQueryFilters.length]);

  const handleFeatureClick = (e: React.MouseEvent<HTMLButtonElement>) => {
    const feature = { value, featureName };

    if (e.shiftKey) {
      const filteredFeatures = queuedQueryFilters.filter((queuedFilter) => {
        return queuedFilter.value !== value || queuedFilter.featureName !== featureName;
      });
      const featureNotYetQueued = filteredFeatures.length === queuedQueryFilters.length;

      if (featureNotYetQueued) {
        setIsSelected(true);
        updateQueuedQueryFilters([...queuedQueryFilters, feature]);
        return;
      }
      // if queued and selected, deselect. if queued and not yet selected, don't add or remove
      if (isSelected) {
        setIsSelected(false);
        updateQueuedQueryFilters(filteredFeatures);
      }
    } else if (e.ctrlKey || e.metaKey) {
      openNewQueryWindow(addFeaturesToQueryFilter([feature]));
    } else {
      onClick?.();
    }

    return false;
  };

  const getSelectedClass = (): string => {
    if (value == null) {
      return styles.null;
    }

    if (value.length < 7) {
      return styles.selectedShort;
    } else if (value.length < 20) {
      return styles.selectedMedium;
    }

    return styles.selectedLong;
  };

  const renderValue = (value: string | null) => {
    // If the value is null, we want the ui to render the string "null".
    if (value == null) return 'null';
    if (isFeatureNameOnly)
      return (
        <Text size={TextSizes.H6} color={TextColors.LIGHT_HEADINGS_PRIMARY} weight={TextWeights.BOLD}>
          {featureName}
        </Text>
      );
    if (featureName !== 'ActionName') return value;

    return (
      <Text size={TextSizes.H6} color={TextColors.LIGHT_HEADINGS_PRIMARY} weight={TextWeights.BOLD}>
        {value}
      </Text>
    );
  };

  const getFeatureTitle = () => {
    if (isFeatureNameOnly) return featureName;
    if (isEntity) return '';
    return value ?? 'null';
  };

  return (
    <OspreyButton
      className={classNames(className, { [getSelectedClass()]: isSelected })}
      style={{ padding: 0 }}
      onClick={handleFeatureClick}
      color={value == null ? ButtonColors.LINK_DISABLED : ButtonColors.LINK_BLUE}
      height={ButtonHeights.SHORT}
      weight={isEntity ? ButtonWeights.SEMIBOLD : ButtonWeights.NORMAL}
      title={getFeatureTitle()}
      textSelectable={textSelectable}
    >
      {renderValue(value)}
    </OspreyButton>
  );
};

type FeatureProps = CommonFeatureProps & {
  value: unknown;
};

const Feature = ({ value, ...props }: FeatureProps): React.ReactElement => {
  return <SelectableFeature value={value == null ? null : String(value)} {...props} />;
};

export default Feature;
