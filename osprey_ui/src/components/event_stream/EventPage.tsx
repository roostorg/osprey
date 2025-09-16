import * as React from 'react';
import { useParams } from 'react-router-dom';

import EventDetailsPanel from './EventDetailsPanel';
import { FeatureLocation } from '../../types/ConfigTypes';
import { getFeatureLocations } from '../../actions/EventActions';

interface Params {
  eventId: string;
}

const EventPage: React.FC = () => {
  const { eventId } = useParams<Params>();
  const [featureLocations, setFeatureLocations] = React.useState<FeatureLocation[] | undefined>([]);

  React.useEffect(() => {
    (async () => {
      const res = await getFeatureLocations();
      setFeatureLocations(res.locations);
    })();
  }, []);

  return <EventDetailsPanel eventId={eventId} featureLocations={featureLocations} />;
};

export default EventPage;
