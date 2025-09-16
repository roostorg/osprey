import * as React from 'react';
import { ResizeObserverEntry } from '@juggle/resize-observer';
import { Spin } from 'antd';
import { debounce, findIndex } from 'lodash';

import { getEventCountsByFeatureForEntityQuery } from '../../actions/EntityActions';
import usePromiseResult from '../../hooks/usePromiseResult';
import useResizeObserver from '../../hooks/useResizeObserver';
import useApplicationConfigStore from '../../stores/ApplicationConfigStore';
import useEntityStore from '../../stores/EntityStore';
import useQueryStore from '../../stores/QueryStore';
import DropdownMenu, { MenuOption } from '../../uikit/DropdownMenu';
import Text, { TextColors, TextSizes, TextWeights } from '../../uikit/Text';
import ArrowIcon from '../../uikit/icons/ArrowIcon';
import { Placements } from '../../utils/DOMUtils';
import { renderFromPromiseResult } from '../../utils/PromiseResultUtils';
import { pluralize } from '../../utils/StringUtils';
import FeatureName from '../common/FeatureName';
import FilterPill from '../common/FilterPill';

import styles from './EntityFeatureFilters.module.css';

const DROPDOWN_MENU_WIDTH_WITH_MARGIN = 70;

const Divider = () => <div className={styles.divider} />;

function sortFeatureFiltersByEventVolume([_nameA, countA]: [string, number], [_nameB, countB]: [string, number]) {
  return countB - countA;
}

const debouncedWidthCheck = (setHiddenFilterIndex: Function, setPreviousWidth: Function) =>
  debounce(() => {
    setHiddenFilterIndex(-1);
    setPreviousWidth(window.innerWidth);
  }, 100)();

const EntityFeatureFilters = () => {
  const entityFeatureFilters = useQueryStore((state) => state.entityFeatureFilters);
  const updateEntityFeatureFilters = useQueryStore((state) => state.updateEntityFeatureFilters);

  const eventCountByFeature = useEntityStore((state) => state.eventCountByFeature);
  const totalEventCount = useEntityStore((state) => state.totalEventCount);

  const [hiddenFilterIndex, setHiddenFilterIndex] = React.useState(-1);
  const [previousWidth, setPreviousWidth] = React.useState(window.innerWidth);

  const [containerElement, setContainerElement] = React.useState<HTMLElement | null>(null);

  const contentRef = React.useCallback((node: HTMLElement | null) => {
    if (node != null) {
      setContainerElement(node);
    }
  }, []);

  const handleResize = React.useCallback(
    (entry: ResizeObserverEntry) => {
      // If window is growing, render all of the filters once so we can
      // measure how many should be in the dropdownMenu.
      if (previousWidth < window.innerWidth) {
        debouncedWidthCheck(setHiddenFilterIndex, setPreviousWidth);
        return;
      }

      const index = findIndex(
        entry.target.children,
        (node) => node.getBoundingClientRect().right + DROPDOWN_MENU_WIDTH_WITH_MARGIN >= window.innerWidth
      );

      if (index !== -1 && index !== hiddenFilterIndex) {
        setHiddenFilterIndex(index);
      }

      setPreviousWidth(window.innerWidth);
    },
    [previousWidth, hiddenFilterIndex, setPreviousWidth, setHiddenFilterIndex]
  );

  useResizeObserver(containerElement, handleResize);

  const handleFeatureClick = (featureName: string) => {
    const updatedEntityFeatureFilters = new Set([...entityFeatureFilters]);

    if (updatedEntityFeatureFilters.has(featureName)) {
      updatedEntityFeatureFilters.delete(featureName);
    } else {
      updatedEntityFeatureFilters.add(featureName);
    }

    updateEntityFeatureFilters(updatedEntityFeatureFilters);
  };

  const renderFeatureFilter = ([featureName, count]: [string, number], className: string) => (
    <FilterPill
      onClick={handleFeatureClick}
      filterName={featureName}
      count={count}
      key={featureName}
      isSelected={entityFeatureFilters.has(featureName)}
      className={className}
    />
  );

  const renderFeatureFilters = () => {
    const sortedFeatureNamesToCounts = Object.entries(eventCountByFeature)
      .filter(([_, count]) => count > 0)
      .sort(sortFeatureFiltersByEventVolume);

    // Filters that are past the hiddenFilterIndex are off screen
    // and should be placed in the dropdown menu.
    let filtersToShow = sortedFeatureNamesToCounts;
    let filtersToHide: Array<[string, number]> = [];

    if (hiddenFilterIndex > -1) {
      filtersToShow = sortedFeatureNamesToCounts.slice(0, hiddenFilterIndex);
      filtersToHide = sortedFeatureNamesToCounts.slice(hiddenFilterIndex);
    }

    const featureFilters = filtersToShow.map((val) => renderFeatureFilter(val, styles.inlineFilter));

    if (filtersToHide.length > 0) {
      const dropdownMenuOptions = filtersToHide.map(([featureName, count]) => ({
        label: FeatureName,
        data: { featureName, count },
      }));

      featureFilters.push(
        <DropdownMenu<{ featureName: string; count: number }>
          className={styles.dropdownMenu}
          placement={Placements.BOTTOM_RIGHT}
          key="dropdown"
          renderOption={(option: MenuOption<{ featureName: string; count: number }>) =>
            renderFeatureFilter([option.data.featureName, option.data.count], styles.menuFeatureFilter)
          }
          // @ts-expect-error react-18-upgrade
          options={dropdownMenuOptions}
        >
          <Text
            className={styles.featureFilterMenu}
            size={TextSizes.SMALL}
            weight={TextWeights.SEMIBOLD}
            color={TextColors.LIGHT_HEADINGS_SECONDARY}
          >
            More
            <ArrowIcon width={12} className={styles.menuArrow} />
          </Text>
        </DropdownMenu>
      );
    }

    return featureFilters;
  };

  return (
    <Text
      size={TextSizes.SMALL}
      weight={TextWeights.SEMIBOLD}
      color={TextColors.LIGHT_HEADINGS_SECONDARY}
      className={styles.featureFiltersContainer}
    >
      <Divider />
      {`${totalEventCount.toLocaleString()} ${pluralize('event', totalEventCount)}`}
      <Divider />
      <div className={styles.inlineFilter}>FILTER:</div>
      <div ref={contentRef} className={styles.featureFilters}>
        {renderFeatureFilters()}
      </div>
    </Text>
  );
};

interface EntityFeatureFiltersWrapperProps {
  entityType: string;
}

const EntityFeatureFiltersWrapper = ({ entityType }: EntityFeatureFiltersWrapperProps) => {
  const entityToFeatureSetMapping = useApplicationConfigStore((state) => state.entityToFeatureSetMapping);
  const executedQuery = useQueryStore((state) => state.executedQuery);

  const entityFeatureSet = React.useMemo(() => {
    return entityType == null ? [] : [...(entityToFeatureSetMapping.get(entityType) ?? [])];
  }, [entityToFeatureSetMapping, entityType]);

  const eventCountByFeatureResults = usePromiseResult(
    () => getEventCountsByFeatureForEntityQuery(executedQuery, [...entityFeatureSet]),
    [executedQuery, entityType]
  );

  return renderFromPromiseResult(eventCountByFeatureResults, () => <EntityFeatureFilters />, {
    renderResolving: () => <Spin style={{ margin: '0 auto' }} size="small" />,
  });
};

export default EntityFeatureFiltersWrapper;
