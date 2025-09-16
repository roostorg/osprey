import * as React from 'react';
import { SelectOutlined } from '@ant-design/icons';
import { Button } from 'antd';
import classNames from 'classnames';

import useApplicationConfigStore from '../../stores/ApplicationConfigStore';
import OspreyButton from '../../uikit/OspreyButton';

import styles from './ExternalLinkButton.module.css';

interface ExternalLinkButtonProps {
  entityType: string;
  entityId: string;
  className?: string;
  icon?: boolean;
}

const ExternalLinkButton = ({ entityType, entityId, className, icon = false }: ExternalLinkButtonProps) => {
  const externalLinks = useApplicationConfigStore((state) => state.externalLinks);
  const externalLink = externalLinks.get(entityType);
  if (externalLink == null) return null;
  const externalLinkFormatted = externalLink.replace('{entity_id}', encodeURIComponent(entityId));

  if (icon) {
    return (
      <Button
        className={classNames(styles.iconWrapper, className)}
        icon={<SelectOutlined className={styles.icon} />}
        style={{ height: 14, width: 14 }}
        type="link"
        target="_blank"
        href={externalLinkFormatted}
      />
    );
  }

  return (
    <OspreyButton style={{ paddingTop: 6 }} className={className} target="_blank" href={externalLinkFormatted}>
      More Info
    </OspreyButton>
  );
};

export default ExternalLinkButton;
