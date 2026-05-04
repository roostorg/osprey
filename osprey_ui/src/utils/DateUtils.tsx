import dayjs, { type Dayjs, type ManipulateType } from 'dayjs';

import { IntervalOptions, MomentRangeValues } from '../types/QueryTypes';

import { DATE_FORMAT } from '../Constants';

const DURATIONS: readonly ManipulateType[] = ['month', 'week', 'day', 'hour'];

export function formatUtcTimestamp(timestamp: string | Dayjs): string {
  return dayjs.utc(timestamp).format(DATE_FORMAT);
}

export function localizeAndFormatTimestamp(timestamp: string | Dayjs): string {
  return dayjs.utc(timestamp).tz(dayjs.tz.guess()).format(DATE_FORMAT);
}

export function isTimestampPast(timestamp: string): boolean {
  return dayjs.utc(timestamp).isBefore(dayjs.utc());
}

export function getIntervalFromDateRange({ start, end }: { start: string; end: string }): MomentRangeValues | null {
  const dayjsDuration = dayjs.duration(dayjs(start).diff(dayjs(end)));
  let unit: string | null = null;
  let numUnits = 0;

  for (const timeUnit of DURATIONS) {
    const num = Math.abs(dayjsDuration.as(timeUnit));
    if (num % 1 === 0) {
      unit = timeUnit;
      numUnits = num;
      break;
    }
  }

  if (unit == null) return null;

  const intervalOption = Object.keys(IntervalOptions).find((key) => {
    const { durationConstructor: option } = IntervalOptions[key as MomentRangeValues];
    return option[0] === numUnits && option[1] === unit;
  });

  return (intervalOption as MomentRangeValues) ?? null;
}
