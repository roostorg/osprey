import { isEqual } from 'lodash';
import moment from 'moment';

import useApplicationConfigStore from '../stores/ApplicationConfigStore';
import { Feature } from '../types/EntityTypes';
import { DefaultIntervals, IntervalOptions, BaseQuery, TopNTable, QueryState } from '../types/QueryTypes';
export const CUSTOM_RANGE_OPTION = 'custom';

export function isEmptyDateRange(start: string, end: string): boolean {
  return start === '' || end === '';
}

export function getQueryDateRange(
  interval: DefaultIntervals,
  dateRange: { start: string; end: string } = { start: '', end: '' }
): { start: string; end: string } {
  if (interval == null) {
    throw new Error('Default range cannot be null');
  }

  if (interval !== CUSTOM_RANGE_OPTION) {
    const {
      durationConstructor: [amount, unit],
    } = IntervalOptions[interval];

    return {
      start: moment.utc().subtract(amount, unit).format(),
      end: moment.utc().format(),
    };
  }

  const { start, end } = dateRange;

  if (isEmptyDateRange(start, end)) {
    throw new Error('Date range must be specified when using custom interval');
  }

  return { start, end };
}

/**
 * This attempts to format/render a value based on the string representation of types that the server sends down.
 * This representation is defined in api, but briefly looks like:
 *
 * - The string value 'str', 'int', 'float', or 'bool' for scalar types
 * - A '?' suffix for optional types
 * - A 'list<...>' prefix/suffix for list types
 *
 * Note that this is incomplete (namely does not handle list types currently) and falls back to just leaving the value
 * as is if the type is unknown.
 */
const renderValueWithType = (value: string | null, type: string | undefined): string => {
  if (value == null) {
    return 'None';
  }

  if (type !== undefined) {
    if (type.endsWith('?')) {
      if (value === 'null') {
        return 'None';
      } else {
        return renderValueWithType(value, type.slice(0, type.length - 1));
      }
    } else if (type === 'str') {
      const escapedId = value.replace(`'`, `\\'`);
      return `'${escapedId}'`;
    } else if (type === 'int' || type === 'float') {
      return value;
    } else if (type === 'bool') {
      if (value === 'true') {
        return 'True';
      } else if (value === 'false') {
        return 'False';
      }
    }
  }

  // Best effort, just return the thing.
  // TODO - Better system for warning that we have an unknown type?
  console.warn(`Unknown feature value type '${type}'!`); // eslint-disable-line no-console
  return value;
};

export function addFeaturesToQueryFilter(featureAdditions: Feature[], queryFilter: string = ''): string {
  const { featureNameToValueTypeMapping } = useApplicationConfigStore.getState();

  const groupedByFeatureName = featureAdditions.reduce<{ [key: string]: Array<string | null> }>(
    (groupedAdditions, addition) => {
      if (Object.prototype.hasOwnProperty.call(groupedAdditions, addition.featureName)) {
        groupedAdditions[addition.featureName].push(addition.value);
      } else {
        groupedAdditions[addition.featureName] = [addition.value];
      }
      return groupedAdditions;
    },
    {}
  );

  const additions = Object.entries(groupedByFeatureName).reduce((newQueryFilter, [featureName, featureIds]) => {
    let newFilter = '';
    const featureIdType = featureNameToValueTypeMapping.get(featureName);
    const renderedIds = featureIds.map((id) => renderValueWithType(id, featureIdType));

    if (renderedIds.length > 1) {
      newFilter = `${featureName} in [${renderedIds.join(', ')}]`;
    } else {
      newFilter = `${featureName} == ${renderedIds[0]}`;
    }

    const addition = newQueryFilter === '' ? newFilter : ` and ${newFilter}`;
    return newQueryFilter + addition;
  }, '');

  return queryFilter === '' ? additions : queryFilter + ` and ${additions}`;
}

export function baseQueryEquals(a: BaseQuery, b: BaseQuery): boolean {
  return (
    moment(a.start).isSame(b.start) &&
    moment(a.end).isSame(b.end) &&
    a.interval === b.interval &&
    a.queryFilter === b.queryFilter
  );
}

export function topNEquals(a: Map<string, TopNTable>, b: Map<string, TopNTable>): boolean {
  return a.size === b.size && isEqual(a, b);
}

export function queryStateEquals(a: QueryState, b: QueryState): boolean {
  return (
    baseQueryEquals(a.executedQuery, b.executedQuery) &&
    topNEquals(a.topNTables, b.topNTables) &&
    a.sortOrder === b.sortOrder
  );
}
