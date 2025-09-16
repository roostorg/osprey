import moment, { Moment } from 'moment-timezone';

import { IntervalOptions, MomentRangeValues } from '../types/QueryTypes';

import { DATE_FORMAT } from '../Constants';

const DURATIONS = ['month', 'week', 'day', 'hour'];

export function formatUtcTimestamp(timestamp: string | Moment): string {
  return moment.utc(timestamp).format(DATE_FORMAT);
}

export function localizeAndFormatTimestamp(timestamp: string | Moment): string {
  return moment.utc(timestamp).tz(moment.tz.guess()).format(DATE_FORMAT);
}

export function isTimestampPast(timestamp: string): boolean {
  return moment.utc(timestamp).isBefore(moment.utc());
}

export function getIntervalFromDateRange({ start, end }: { start: string; end: string }): MomentRangeValues | null {
  const momentDuration = moment.duration(moment(start).diff(moment(end)));
  let unit: moment.unitOfTime.Base | null = null;
  let numUnits = 0;

  for (const timeUnit of DURATIONS) {
    const unitOfTime = timeUnit as moment.unitOfTime.Base;
    const num = Math.abs(momentDuration.as(unitOfTime));
    if (num % 1 === 0) {
      unit = unitOfTime;
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
