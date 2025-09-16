import React, { useState } from 'react';
import {
  ExclamationCircleOutlined,
  AimOutlined,
  ImportOutlined,
  ExportOutlined,
  SearchOutlined,
  CloseCircleOutlined,
} from '@ant-design/icons';
import { Menu, List, Input, Switch, Descriptions } from 'antd';
import classNames from 'classnames';
import hljs from 'highlight.js';

import { getDetailedEventFeatures } from '../../actions/EventActions';
import usePromiseResult from '../../hooks/usePromiseResult';
import { StoredExecutionResult } from '../../types/QueryTypes';
import Text, { TextSizes, TextColors } from '../../uikit/Text';
import { formatUtcTimestamp, localizeAndFormatTimestamp } from '../../utils/DateUtils';
import { renderFromPromiseResult } from '../../utils/PromiseResultUtils';
import Feature from '../common/Feature';
import SourcesLink from '../common/SourcesLink';

import styles from './EventDetailsPanel.module.css';
import 'highlight.js/styles/github.css';
import { wrapEntityKeysWithFeatureLocationsMenu } from '../../utils/EntityUtils';
import { FeatureLocation } from '../../types/ConfigTypes';
import Tooltip, { TooltipSizes } from '../../uikit/Tooltip';

enum EventDataTabs {
  EventDetails = 'EVENT_DETAILS',
  ActionData = 'ACTION_DATA',
  ErrorTraces = 'ERROR_TRACES',
  ExtractedFeatures = 'EXTRACTED_FEATURES',
}

interface EventDetailsPanelProps {
  eventId: string;
  featureLocations: FeatureLocation[] | undefined;
}

interface MenuBarProps {
  onClick: (params: { key: React.Key }) => void;
  currentTab: EventDataTabs;
  storedExecutionResult: StoredExecutionResult;
}

const MenuBar = ({ onClick, currentTab, storedExecutionResult }: MenuBarProps) => {
  const numErrorTraces = storedExecutionResult.error_traces.length;
  return (
    <Menu className={styles.__invalid_menu} onClick={onClick} selectedKeys={[currentTab]} mode="horizontal">
      <Menu.Item key={EventDataTabs.EventDetails} icon={<AimOutlined />}>
        Overview
      </Menu.Item>
      {numErrorTraces > 0 ? (
        <Menu.Item key={EventDataTabs.ErrorTraces} icon={<ExclamationCircleOutlined />}>
          <b>{numErrorTraces}</b> Error{numErrorTraces === 1 ? '' : 's'}
        </Menu.Item>
      ) : null}
      <Menu.Item key={EventDataTabs.ActionData} icon={<ImportOutlined />}>
        Input
      </Menu.Item>
      <Menu.Item key={EventDataTabs.ExtractedFeatures} icon={<ExportOutlined />}>
        Output
      </Menu.Item>
    </Menu>
  );
};

const Json = ({ content }: { content: any }) => {
  const code = hljs.highlight('json', JSON.stringify(content, null, 2), true);
  return (
    <pre className={styles.jsonWrap}>
      <code className={classNames(styles.json, 'json')} dangerouslySetInnerHTML={{ __html: code.value }} />
    </pre>
  );
};

interface ClearSearchButtonProps {
  searchValue: string;
  onClearSearchClick: () => void;
}

const ClearSearchButton = ({ searchValue, onClearSearchClick }: ClearSearchButtonProps) => {
  if (searchValue.length > 0) {
    return <CloseCircleOutlined onClick={() => onClearSearchClick()} />;
  }
  return null;
};

interface EventDetailsProps {
  storedExecutionResult: StoredExecutionResult;
  featureLocations: FeatureLocation[] | undefined;
}

const EventDetails = ({ storedExecutionResult, featureLocations }: EventDetailsProps) => {
  const extractedFeatures = storedExecutionResult.extracted_features;
  const [value, setValue] = useState('');
  const [includeNullValues, setIncludeNullValues] = useState(true);

  const shouldDisplayRow = (featureName: string, featureValue: string) => {
    return featureName.toLowerCase().includes(value.toLowerCase()) && (includeNullValues || featureValue !== null);
  };

  return (
    <>
      <div>
        <Text size={TextSizes.H5}>ACTION ID: {storedExecutionResult.id}</Text>
        <Feature
          className={styles.eventName}
          featureName="ActionName"
          value={storedExecutionResult.extracted_features.ActionName}
        />
        <div className={styles.timestampContainer}>
          <Text tag="span" color={TextColors.LIGHT_SECONDARY} size={TextSizes.XSMALL} className={styles.timestamp}>
            {localizeAndFormatTimestamp(storedExecutionResult.timestamp)}
          </Text>
          <Tooltip
            content={
              <Text tag="span" color={TextColors.LIGHT_SECONDARY} className={styles.timestamp}>
                {formatUtcTimestamp(storedExecutionResult.timestamp)}
              </Text>
            }
            size={TooltipSizes.SMALL}
          >
            <span>ⓘ</span>
          </Tooltip>
        </div>
      </div>
      <div className={styles.tableFilters}>
        <Input
          className={styles.filterSearch}
          placeholder="Filter feature list"
          value={value}
          prefix={<SearchOutlined />}
          suffix={<ClearSearchButton searchValue={value} onClearSearchClick={() => setValue('')} />}
          onChange={(e) => setValue(e.target.value)}
        />
        <div className={styles.switchContainer}>
          <Switch checked={!includeNullValues} onClick={() => setIncludeNullValues(!includeNullValues)} />
          <div className={styles.switchText}>Hide empty features</div>
        </div>
      </div>
      <Descriptions size="small" column={1} bordered>
        {Object.entries(extractedFeatures).map(([featureName, featureValue]) => {
          return shouldDisplayRow(featureName, featureValue) ? (
            <Descriptions.Item
              label={wrapEntityKeysWithFeatureLocationsMenu(featureName, featureLocations)}
              key={featureName}
            >
              <Feature featureName={featureName} value={featureValue} textSelectable={featureValue != null} />
            </Descriptions.Item>
          ) : null;
        })}
      </Descriptions>
    </>
  );
};

interface ErrorTracesProps {
  storedExecutionResult: StoredExecutionResult;
}

const ErrorTraces = ({ storedExecutionResult }: ErrorTracesProps): React.ReactElement => {
  return (
    <List
      itemLayout="horizontal"
      dataSource={storedExecutionResult.error_traces}
      renderItem={(trace, idx) => {
        const [path, content] = trace.rules_source_location.split(' - ');

        return (
          <List.Item>
            <List.Item.Meta
              className={styles.errorTrace}
              title={
                <>
                  <b>{`#${idx + 1}: `}</b>
                  <SourcesLink path={path} /> - <code>{content}</code>
                </>
              }
              description={<pre>{trace.traceback}</pre>}
            />
          </List.Item>
        );
      }}
    />
  );
};

interface EventStreamContentProps {
  storedExecutionResult: StoredExecutionResult;
  featureLocations: FeatureLocation[] | undefined;
}

const EventDetailsPanelContent = ({ storedExecutionResult, featureLocations }: EventStreamContentProps) => {
  const [currentTab, setCurrentTab] = React.useState<EventDataTabs>(EventDataTabs.EventDetails);

  const handleMenuClick = React.useCallback((e: { key: React.Key }) => {
    setCurrentTab(e.key as EventDataTabs);
  }, []);

  const renderStreamContent = () => {
    switch (currentTab) {
      case EventDataTabs.EventDetails:
        return <EventDetails storedExecutionResult={storedExecutionResult} featureLocations={featureLocations} />;
      case EventDataTabs.ActionData:
        return <Json content={storedExecutionResult.action_data} />;
      case EventDataTabs.ErrorTraces:
        return <ErrorTraces storedExecutionResult={storedExecutionResult} />;
      case EventDataTabs.ExtractedFeatures:
        return <Json content={storedExecutionResult.extracted_features} />;
    }
  };

  return (
    <>
      <MenuBar currentTab={currentTab} onClick={handleMenuClick} storedExecutionResult={storedExecutionResult} />
      <div className={styles.eventContents}>{renderStreamContent()}</div>
    </>
  );
};

const EventDetailsPanel = ({ eventId, featureLocations }: EventDetailsPanelProps) => {
  const panelContent = renderFromPromiseResult(
    usePromiseResult(() => getDetailedEventFeatures(eventId), [eventId]),
    (storedExecutionResult) => (
      <EventDetailsPanelContent storedExecutionResult={storedExecutionResult} featureLocations={featureLocations} />
    )
  );

  return <div className={styles.panel}>{panelContent}</div>;
};

export default EventDetailsPanel;
