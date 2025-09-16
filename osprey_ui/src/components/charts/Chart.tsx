import * as React from 'react';

import useQueryStore from '../../stores/QueryStore';
import Timeseries from '../timeseries/Timeseries';
import { Input } from 'antd';
import { Chart as ChartData } from '../../types/QueryTypes';
import OspreyButton, { ButtonColors } from '../../uikit/OspreyButton';

import styles from './Chart.module.css';

const Chart = ({ index }: { index: number }) => {
  const [chart, updateChart] = useQueryStore((state) => [state.charts[index], state.updateChart]);
  const [text, setText] = React.useState(chart.query);

  React.useEffect(() => setText(chart.query), [chart]);

  return (
    <div className={styles.chartWrapper}>
      <Timeseries extraQuery={chart.query} />
      <div className={styles.inputWrapper}>
        <Input.TextArea
          className={styles.textArea}
          value={text}
          onChange={(e) => setText(e.target.value)}
          spellCheck="false"
        />
        <div className={styles.buttonWrapper}>
          <OspreyButton onClick={() => updateChart(index, new ChartData(chart.query, true))}>Yeet</OspreyButton>
          <OspreyButton
            color={ButtonColors.DARK_BLUE}
            onClick={() => updateChart(index, new ChartData(text, chart.deleted))}
          >
            Submit
          </OspreyButton>
        </div>
      </div>
    </div>
  );
};

export default Chart;
