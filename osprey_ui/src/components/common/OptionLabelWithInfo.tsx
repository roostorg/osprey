import { InfoCircleOutlined } from '@ant-design/icons';
import { Tooltip } from 'antd';

import { Key } from 'rc-select/lib/interface/generator';

import { OptionInfoMapping } from '../../stores/ApplicationConfigStore';

import styles from './OptionLabelWithInfo.module.css';

function OptionLabelWithInfo({ option, optionInfoMapping }: { option: Key; optionInfoMapping: OptionInfoMapping }) {
  if (typeof option !== 'number' && optionInfoMapping.get(option)) {
    return (
      <div className={styles.optionLabelWithInfo} key={option}>
        <div className={styles.feature}>{option}</div>
        <Tooltip title={optionInfoMapping.get(option)} placement="right" overlayStyle={{ maxWidth: 500 }}>
          <InfoCircleOutlined />
        </Tooltip>
      </div>
    );
  } else {
    return <>{option}</>;
  }
}

export default OptionLabelWithInfo;
