import * as React from 'react';
import { Link } from 'react-router-dom';

import useApplicationConfigStore from '../../stores/ApplicationConfigStore';

interface RichDescriptorValueProps {
  featureName: string;
  featureVal: string;
}

const RichDescriptorValue = ({ featureName, featureVal }: RichDescriptorValueProps) => {
  const featureNameToEntityTypeMapping = useApplicationConfigStore((state) => state.featureNameToEntityTypeMapping);
  const entityType = featureNameToEntityTypeMapping.get(featureName);

  if (entityType != null) {
    const entityRoutePath = `/entity/${entityType}/${featureVal}`;
    return <Link to={{ pathname: entityRoutePath }}>{featureVal}</Link>;
  }

  return <>{featureVal}</>;
};

export default RichDescriptorValue;
