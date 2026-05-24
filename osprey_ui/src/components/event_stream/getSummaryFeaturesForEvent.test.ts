import DefaultFeature from '../../models/DefaultFeature';
import { OspreyEvent } from '../../types/QueryTypes';

import { getSummaryFeaturesForEvent } from './getSummaryFeaturesForEvent';

const makeEvent = (extracted: Record<string, unknown>): OspreyEvent => {
  return {
    timestamp: '2025-01-01T00:00:00Z',
    id: 'evt-1',
    extracted_features: extracted,
  };
};

describe('getSummaryFeaturesForEvent', () => {
  it('returns configured defaults when an action-name pattern matches', () => {
    const defaults = [
      new DefaultFeature(['LoginAction'], ['UserId', 'IpAddress']),
      new DefaultFeature(['NotApplicable'], ['ShouldNotAppear']),
    ];
    const event = makeEvent({ ActionName: 'LoginAction', UserId: '123', IpAddress: '10.0.0.1' });

    const result = getSummaryFeaturesForEvent(event, defaults);

    expect(result).toEqual([['UserId', 'IpAddress']]);
  });

  it('returns all matching default blocks when multiple apply', () => {
    const defaults = [
      new DefaultFeature(['*'], ['UserId']),
      new DefaultFeature(['SendMessage'], ['ChannelId', 'GuildId']),
    ];
    const event = makeEvent({ ActionName: 'SendMessage', UserId: '1', ChannelId: 'c', GuildId: 'g' });

    const result = getSummaryFeaturesForEvent(event, defaults);

    expect(result).toEqual([['UserId'], ['ChannelId', 'GuildId']]);
  });

  it('falls back to every extracted feature key (excluding ActionName) when no defaults are configured', () => {
    const event = makeEvent({ ActionName: 'AnythingAction', UserId: '42', ChannelId: 'c1', GuildId: 'g1' });

    const result = getSummaryFeaturesForEvent(event, []);

    expect(result).toHaveLength(1);
    expect(new Set(result[0])).toEqual(new Set(['UserId', 'ChannelId', 'GuildId']));
    expect(result[0]).not.toContain('ActionName');
  });

  it('falls back to event keys when defaults exist but none match the action', () => {
    const defaults = [new DefaultFeature(['SomeOtherAction'], ['IrrelevantField'])];
    const event = makeEvent({ ActionName: 'UnmatchedAction', UserId: 'u', GuildId: 'g' });

    const result = getSummaryFeaturesForEvent(event, defaults);

    expect(result).toHaveLength(1);
    expect(new Set(result[0])).toEqual(new Set(['UserId', 'GuildId']));
  });

  it('returns an empty list when the event has only ActionName and no defaults apply', () => {
    const event = makeEvent({ ActionName: 'NakedAction' });

    const result = getSummaryFeaturesForEvent(event, []);

    expect(result).toEqual([]);
  });

  it('returns an empty list when the event has no extracted features at all', () => {
    const event = makeEvent({});

    const result = getSummaryFeaturesForEvent(event, []);

    expect(result).toEqual([]);
  });
});
