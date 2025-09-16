import * as React from 'react';
import Panel from '../common/Panel';
import TimeseriesIcon from '../../uikit/icons/TimeseriesIcon';
import OspreyButton from '../../uikit/OspreyButton';
import useQueryStore from '../../stores/QueryStore';
import { Chart } from '../../types/QueryTypes';
import { Empty } from 'antd';
import styles from './Charts.module.css';
import ChartComponent from './Chart';

const Charts: React.FC = () => {
  const [charts, addChart] = useQueryStore((state) => [state.charts, state.addChart]);

  const renderCharts = () => {
    return (
      <div>
        {charts
          .map((chart, index) => ({ chart, index }))
          .reverse()
          .filter(({ chart, index }) => !chart.deleted)
          .map(({ chart, index }) => {
            return <ChartComponent key={index} index={index} />;
          })}
      </div>
    );
  };

  return (
    <Panel
      className={styles.chartsPanel}
      title="Extra Charts"
      icon={<TimeseriesIcon />}
      titleRight={<OspreyButton onClick={() => addChart(new Chart('', false))}>Add Chart</OspreyButton>}
    >
      {renderCharts()}
    </Panel>
  );
};

export default Charts;
