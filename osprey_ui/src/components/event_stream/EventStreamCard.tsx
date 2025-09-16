import * as React from 'react';
import { Empty } from 'antd';
import NewWindow from 'react-new-window';
import { useParams } from 'react-router-dom';

import useApplicationConfigStore from '../../stores/ApplicationConfigStore';
import { EntityViewParams } from '../../stores/EntityStore';
import { OspreyEvent } from '../../types/QueryTypes';
import OspreyButton from '../../uikit/OspreyButton';
import Text, { TextColors } from '../../uikit/Text';
import { formatUtcTimestamp, localizeAndFormatTimestamp } from '../../utils/DateUtils';
import { wrapEntityKeysWithFeatureLocationsMenu, wrapEntityValuesWithLabelMenu } from '../../utils/EntityUtils';
import CopyLinkButton from '../common/CopyLinkButton';
import Feature from '../common/Feature';
import PropertyTable, { Entries } from '../common/PropertyTable';

import styles from './EventStreamCard.module.css';
import { FeatureLocation } from '../../types/ConfigTypes';
import Tooltip, { TooltipSizes } from '../../uikit/Tooltip';

interface EventStreamCardProps {
  eventDetails: OspreyEvent;
  selectedFeatures: Array<readonly string[]>;
  featureLocations: Array<FeatureLocation> | undefined;
  isListView: boolean;
}

const EventStreamCard = ({ eventDetails, selectedFeatures, featureLocations, isListView }: EventStreamCardProps) => {
  const [showWindow, setShowWindow] = React.useState(false);
  const { entityId, entityType } = useParams<EntityViewParams>();
  const decodedEntityId = entityId != null ? decodeURIComponent(entityId) : null;
  const decodedEntityType = entityType != null ? decodeURIComponent(entityType) : null;
  const featureNameToEntityTypeMapping = useApplicationConfigStore((state) => state.featureNameToEntityTypeMapping);

  const handleShowWindow = () => {
    setShowWindow(true);
  };

  const handleWindowClose = () => {
    setShowWindow(false);
  };

  const renderDescriptionBlock = (features: readonly string[]) => {
    const entries: Record<string, string> = {};

    features.forEach((feature) => {
      if (Object.prototype.hasOwnProperty.call(eventDetails.extracted_features, feature)) {
        entries[feature] = eventDetails.extracted_features[feature];
      }
    });

    if (Object.keys(entries).length === 0) return null;

    return entries;
  };

  const renderContent = () => {
    if (isListView) return null;

    const features = selectedFeatures.map(renderDescriptionBlock).filter((block) => block != null) as Entries[];
    const isEmpty = features.length === 0;
    let entityViewProps = {};

    if (decodedEntityId != null && decodedEntityType != null) {
      entityViewProps = {
        shouldHighlightCell: ([key, value]: [string, string]) => {
          if (value == null) return false;
          const featureEntityType = featureNameToEntityTypeMapping.get(key);
          return decodedEntityType === featureEntityType && decodedEntityId.toString() === value.toString();
        },
      };
    }

    return isEmpty ? (
      <Empty imageStyle={{ height: 40 }} description="No data for selected features" />
    ) : (
      <PropertyTable
        className={styles.contentTable}
        entries={features}
        columns={3}
        renderKey={([key]) => wrapEntityKeysWithFeatureLocationsMenu(key, featureLocations)}
        renderValue={([key, value]) => wrapEntityValuesWithLabelMenu(value, key)}
        {...entityViewProps}
      />
    );
  };

  const eventUrl = `${document.location.origin}/events/${eventDetails.id}`;

  const cardTitle = (
    <div className={styles.cardTitle}>
      <Feature
        className={styles.actionName}
        featureName="ActionName"
        value={eventDetails.extracted_features.ActionName}
      />
      <Text tag="span" color={TextColors.LIGHT_SECONDARY} className={styles.timestamp}>
        {localizeAndFormatTimestamp(eventDetails.timestamp)}
      </Text>
      <Tooltip
        content={
          <Text tag="span" color={TextColors.LIGHT_SECONDARY} className={styles.timestamp}>
            {formatUtcTimestamp(eventDetails.timestamp)}
          </Text>
        }
        size={TooltipSizes.SMALL}
      >
        <span>ⓘ</span>
      </Tooltip>
      <CopyLinkButton link={eventUrl} />
    </div>
  );

  return (
    <div className={styles.card}>
      <div className={styles.cardHeader}>
        {cardTitle}
        <OspreyButton onClick={handleShowWindow}>
          See Details
          {showWindow && (
            <NewWindow url={eventUrl} onUnload={handleWindowClose} features={{ width: 800, height: 800 }} />
          )}
        </OspreyButton>
      </div>
      <div>{renderContent()}</div>
    </div>
  );
};

export default EventStreamCard;
