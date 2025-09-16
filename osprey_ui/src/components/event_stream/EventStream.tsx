import * as React from 'react';
import {
  UnorderedListOutlined,
  ProfileOutlined,
  SortAscendingOutlined,
  SortDescendingOutlined,
} from '@ant-design/icons';
import { Empty, List, Skeleton, Tooltip, Spin } from 'antd';
import classNames from 'classnames';
import { memoize } from 'lodash';
import { AutoSizer, List as VList, ListRowProps } from 'react-virtualized';
import shallow from 'zustand/shallow';

import { getFeatureLocations, getScanQueryResults } from '../../actions/EventActions';
import useApplicationConfigStore from '../../stores/ApplicationConfigStore';
import useQueryStore from '../../stores/QueryStore';
import { OspreyEvent, ScanQueryOrder } from '../../types/QueryTypes';
import OspreyButton from '../../uikit/OspreyButton';
import EventStreamIcon from '../../uikit/icons/EventStreamIcon';
import Panel from '../common/Panel';
import EventStreamCard from './EventStreamCard';
import FeatureSelectModal from './FeatureSelectModal';

import styles from './EventStream.module.css';
import { FeatureLocation } from '../../types/ConfigTypes';

const QUERY_ROW_LIMIT = 100;
const HEADER_HEIGHT = 44;
const ADDITIONAL_PADDING = 4;
const ROW_HEIGHT = 44;
const NUM_COLUMNS = 3;

const calculateCardHeight = (currentHeight: number, featureBlock: readonly string[], item: OspreyEvent) => {
  const numFeatures = featureBlock.reduce(
    (acc, feature) => acc + (Object.prototype.hasOwnProperty.call(item.extracted_features, feature) ? 1 : 0),
    0
  );
  const rows = Math.ceil(numFeatures / NUM_COLUMNS);
  return currentHeight + rows * ROW_HEIGHT;
};

const EventStream: React.FC = () => {
  const [
    executedQuery,
    sortOrder,
    entityFeatureFilters,
    applyIfQueryIsCurrent,
    updateSortOrder,
    customSummaryFeatures,
  ] = useQueryStore(
    (state) => [
      state.executedQuery,
      state.sortOrder,
      state.entityFeatureFilters,
      state.applyIfQueryIsCurrent,
      state.updateSortOrder,
      state.customSummaryFeatures,
    ],
    shallow
  );
  const defaultSummaryFeatures = useApplicationConfigStore((state) => state.defaultSummaryFeatures);
  const vlistRef = React.useRef<VList>(null);

  const [isLoading, setIsLoading] = React.useState(false);
  const [eventStream, setEventStream] = React.useState<OspreyEvent[]>([]);
  const [queryOffset, setQueryOffset] = React.useState<string | null>(null);
  const [isListView, setIsListView] = React.useState(false);
  const [featureLocations, setFeatureLocations] = React.useState<FeatureLocation[] | undefined>([]);

  const handleScanQuery = React.useCallback(
    async (currentEvents: OspreyEvent[] = [], newQueryOffset?: string) => {
      setIsLoading(true);
      const { events, offset } = await getScanQueryResults(
        { ...executedQuery, entityFeatureFilters },
        sortOrder,
        QUERY_ROW_LIMIT,
        newQueryOffset
      );

      applyIfQueryIsCurrent(executedQuery, () => {
        setEventStream([...currentEvents, ...events]);
        setQueryOffset(offset);
        setIsLoading(false);
      });
    },
    [executedQuery, sortOrder, entityFeatureFilters, applyIfQueryIsCurrent]
  );

  React.useEffect(() => {
    const { start, end } = executedQuery;
    setEventStream([]);
    setQueryOffset(null);

    if (start !== '' && end !== '') {
      // This should only be called if the query has changed, so reset local state
      handleScanQuery();
    }
  }, [handleScanQuery, executedQuery]);

  React.useEffect(() => {
    (async () => {
      const { locations } = await getFeatureLocations();
      setFeatureLocations(locations);
    })();
  }, []);

  React.useEffect(() => {
    vlistRef.current?.recomputeRowHeights();
  }, [customSummaryFeatures, isListView, vlistRef]);

  const handlePaginatedScanQuery = () => {
    if (eventStream.length === 0 || queryOffset == null || isLoading) return;
    handleScanQuery(eventStream, queryOffset);
  };

  const shouldLoadMoreResults = (index: number): boolean => {
    return eventStream.length - index < 5;
  };

  const getSummaryFeatures = React.useMemo(
    () =>
      memoize((actionName: string) =>
        defaultSummaryFeatures.filter((f) => f.appliesTo(actionName)).map((f) => f.features)
      ),
    [defaultSummaryFeatures]
  );

  const renderListRows = ({ key, index, style, isVisible }: ListRowProps) => {
    if (isVisible && shouldLoadMoreResults(index)) {
      handlePaginatedScanQuery();
    }

    const item = eventStream[index];
    const features =
      customSummaryFeatures == null ? getSummaryFeatures(item.extracted_features.ActionName) : [customSummaryFeatures];

    return (
      <List.Item
        className={classNames(styles.listItemWrapper, { [styles.listView]: isListView })}
        key={key}
        style={style}
      >
        <Skeleton loading={isLoading} active title paragraph={{ rows: 5 }}>
          <EventStreamCard
            selectedFeatures={features}
            featureLocations={featureLocations}
            eventDetails={item}
            isListView={isListView}
          />
        </Skeleton>
      </List.Item>
    );
  };

  const getRowHeight = ({ index }: { index: number }): number => {
    if (isListView) return HEADER_HEIGHT;

    const features =
      customSummaryFeatures == null
        ? getSummaryFeatures(eventStream[index].extracted_features.ActionName)
        : [customSummaryFeatures];

    return features.reduce(
      (height: number, featureBlock: readonly string[]) =>
        calculateCardHeight(height, featureBlock, eventStream[index]),
      HEADER_HEIGHT + ADDITIONAL_PADDING
    );
  };

  const renderVirtualizedList = () => {
    if (eventStream.length === 0) {
      return isLoading ? (
        <Spin size="large" style={{ position: 'absolute', top: 40, left: 0, right: 0 }} />
      ) : (
        <Empty style={{ paddingTop: 40 }} />
      );
    }

    return (
      <AutoSizer>
        {({ width, height }) => (
          <VList
            className={styles.virtualizedList}
            width={width}
            height={height}
            rowCount={eventStream.length}
            rowHeight={getRowHeight}
            rowRenderer={renderListRows}
            ref={vlistRef}
          />
        )}
      </AutoSizer>
    );
  };

  const handleToggleViewType = () => {
    setIsListView(!isListView);
  };

  const handleSetQueryOrder = (order: ScanQueryOrder) => {
    updateSortOrder(order);
  };

  const renderTitleRight = () => {
    const viewSwitchIcon = isListView ? <ProfileOutlined /> : <UnorderedListOutlined />;
    const oppositeView = isListView ? 'card' : 'list';
    const oppositeOrder = sortOrder === ScanQueryOrder.ASCENDING ? ScanQueryOrder.DESCENDING : ScanQueryOrder.ASCENDING;
    const orderIcon = sortOrder === ScanQueryOrder.ASCENDING ? <SortAscendingOutlined /> : <SortDescendingOutlined />;

    return (
      <div className={styles.buttonContainer}>
        <Tooltip title={`Switch to sorting by date ${oppositeOrder} (currently: ${sortOrder})`}>
          <span>
            <OspreyButton
              className={styles.viewSwitchButton}
              icon={orderIcon}
              onClick={() => handleSetQueryOrder(oppositeOrder)}
            />
          </span>
        </Tooltip>
        <Tooltip title={`Switch to ${oppositeView} view`}>
          <span>
            <OspreyButton className={styles.viewSwitchButton} icon={viewSwitchIcon} onClick={handleToggleViewType} />
          </span>
        </Tooltip>
        <FeatureSelectModal />
      </div>
    );
  };

  return (
    <Panel
      className={styles.eventStreamPanel}
      title="Event Stream"
      titleRight={renderTitleRight()}
      icon={<EventStreamIcon />}
    >
      <div className={styles.listWrapper}>
        <div className={styles.eventStreamList}>{renderVirtualizedList()}</div>
      </div>
    </Panel>
  );
};

export default EventStream;
