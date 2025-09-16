import * as React from 'react';

import RichDescriptorValue from './RichDescriptorValue';

import styles from './RichDescription.module.css';

interface RichDescriptionProps {
  description: string;
  features: { [featureName: string]: string };
}

const INTERPOLATE_REG_EX = /{([A-z0-9_]+)}/;
const SPLIT_REG_EX = /(:?{[A-z0-9_]+})/;

const RichDescription = ({ description, features }: RichDescriptionProps) => {
  const interpolatedDescription = description.split(SPLIT_REG_EX).map((part, i) => {
    const matchList = part.match(INTERPOLATE_REG_EX);
    const featureName = matchList != null ? matchList[1] : null;
    const featureVal = featureName != null ? features[featureName] : null;

    if (featureName == null || featureVal == null) return part;

    return (
      <span key={i} title={featureName} className={styles.interpolatedFeature}>
        <RichDescriptorValue featureName={featureName} featureVal={featureVal} />
      </span>
    );
  });
  return <div>{interpolatedDescription}</div>;
};

export default RichDescription;
