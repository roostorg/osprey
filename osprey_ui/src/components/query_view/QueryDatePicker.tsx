import * as React from 'react';
import { EditOutlined } from '@ant-design/icons';
import { DatePicker } from 'antd';
import type { RangePickerProps } from 'antd/es/date-picker';
import dayjs, { type Dayjs } from 'dayjs';

import { DefaultIntervals } from '../../types/QueryTypes';
import Text, { TextColors, TextSizes, TextWeights } from '../../uikit/Text';
import { localizeAndFormatTimestamp } from '../../utils/DateUtils';
import { CUSTOM_RANGE_OPTION, isEmptyDateRange } from '../../utils/QueryUtils';

import { DATE_FORMAT } from '../../Constants';
import styles from './QueryDatePicker.module.css';

interface QueryDatePickerProps {
  onIntervalChange: (interval: DefaultIntervals) => void;
  onDateRangeChange: (dateRange: { start: string; end: string }) => void;
  interval: DefaultIntervals;
  dateRange: { start: string; end: string };
}

type RangePickerValue = [Dayjs, Dayjs];

const QueryDatePicker = ({ onIntervalChange, onDateRangeChange, interval, dateRange }: QueryDatePickerProps) => {
  const handleRangePickerChange: RangePickerProps['onChange'] = (dates, _dateStrings) => {
    if (dates == null) return;

    const start = dates[0];
    const end = dates[1];
    if (start == null || end == null) return;

    // Convert to UTC format for backend API
    onDateRangeChange({ start: start.utc().format(), end: end.utc().format() });
  };

  const handleSwitchToCustomRange = () => {
    onIntervalChange(CUSTOM_RANGE_OPTION);
  };

  const renderQueryIntervalOrDatePicker = () => {
    if (interval === CUSTOM_RANGE_OPTION) {
      const value: RangePickerValue | undefined = isEmptyDateRange(dateRange.start, dateRange.end)
        ? undefined
        : [dayjs(dateRange.start), dayjs(dateRange.end)];

      return (
        <DatePicker.RangePicker
          showTime={{ format: 'hh:mma', use12Hours: true }}
          format={DATE_FORMAT}
          placeholder={['Start Date', 'End Date']}
          onChange={handleRangePickerChange}
          value={value}
        />
      );
    }

    if (isEmptyDateRange(dateRange.start, dateRange.end)) return null;

    return (
      <Text
        className={styles.interval}
        size={TextSizes.SMALL}
        weight={TextWeights.SEMIBOLD}
        color={TextColors.LIGHT_SECONDARY}
      >
        Queried {`${localizeAndFormatTimestamp(dateRange.start)} - ${localizeAndFormatTimestamp(dateRange.end)}`}
        <EditOutlined className={styles.editIcon} onClick={handleSwitchToCustomRange} />
      </Text>
    );
  };

  return <div className={styles.datePickerWrapper}>{renderQueryIntervalOrDatePicker()}</div>;
};

export default QueryDatePicker;
