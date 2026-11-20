import { beforeEach, describe, expect, it } from '@rstest/core';

import useApplicationConfigStore from '../stores/ApplicationConfigStore';

import { addFeaturesToQueryFilter } from './QueryUtils';

describe('addFeaturesToQueryFilter', () => {
  beforeEach(() => {
    useApplicationConfigStore.setState({
      featureNameToValueTypeMapping: new Map([
        ['UserName', 'str'],
        ['Nickname', 'str?'],
      ]),
    });
  });

  it('quotes a plain string value', () => {
    expect(addFeaturesToQueryFilter([{ featureName: 'UserName', value: 'alice' }])).toBe(`UserName == 'alice'`);
  });

  it('escapes every apostrophe, not just the first', () => {
    expect(addFeaturesToQueryFilter([{ featureName: 'UserName', value: `it's o'clock` }])).toBe(
      String.raw`UserName == 'it\'s o\'clock'`
    );
  });

  it('escapes every backslash', () => {
    expect(addFeaturesToQueryFilter([{ featureName: 'UserName', value: String.raw`a\b\\c` }])).toBe(
      String.raw`UserName == 'a\\b\\\\c'`
    );
  });

  it('escapes a backslash immediately before an apostrophe', () => {
    expect(addFeaturesToQueryFilter([{ featureName: 'UserName', value: String.raw`x\'y` }])).toBe(
      String.raw`UserName == 'x\\\'y'`
    );
  });

  it('escapes a value ending in a backslash so it cannot escape the closing quote', () => {
    expect(addFeaturesToQueryFilter([{ featureName: 'UserName', value: 'trailing\\' }])).toBe(
      `UserName == 'trailing\\\\'`
    );
  });

  it('escapes each value of a multi-value filter and of optional string features', () => {
    expect(
      addFeaturesToQueryFilter(
        [
          { featureName: 'UserName', value: `a'b'c` },
          { featureName: 'UserName', value: String.raw`d\e\f` },
          { featureName: 'Nickname', value: `o''k` },
        ],
        `ActionName == 'Login'`
      )
    ).toBe(String.raw`ActionName == 'Login' and UserName in ['a\'b\'c', 'd\\e\\f'] and Nickname == 'o\'\'k'`);
  });
});
