import * as React from 'react';
import { CheckCircleOutlined, LinkOutlined } from '@ant-design/icons';
import { message, Button } from 'antd';
import { copyText } from '../../lib/copyText';

interface CopyLinkButtonProps {
  link: string;
}

const CopyLinkButton = ({ link }: CopyLinkButtonProps) => {
  const [showCopySuccess, setShowCopySuccess] = React.useState(false);

  React.useEffect(() => {
    let timeout: NodeJS.Timeout | undefined;
    if (showCopySuccess) {
      timeout = setTimeout(() => {
        setShowCopySuccess(false);
      }, 1000);
    }

    return () => {
      if (timeout != null) {
        clearTimeout(timeout);
      }
    };
  }, [showCopySuccess]);

  const handleCopyLink = async () => {
    await copyText(link);
    message.success('Link copied to clipboard', 1);
    setShowCopySuccess(true);
  };

  return (
    <Button
      onClick={handleCopyLink}
      icon={showCopySuccess ? <CheckCircleOutlined /> : <LinkOutlined />}
      type="link"
      size="small"
    />
  );
};

export default CopyLinkButton;
