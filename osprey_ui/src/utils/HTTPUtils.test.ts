import { beforeEach, describe, expect, it } from '@rstest/core';
import { AxiosError, AxiosHeaders, AxiosResponse } from 'axios';

import useErrorStore from '../stores/ErrorStore';

import HTTPUtils, { getErrorMessage } from './HTTPUtils';

function responseWith(status: number, data: unknown): AxiosResponse {
  return { status, statusText: '', data, headers: {}, config: { headers: new AxiosHeaders() } };
}

describe('getErrorMessage', () => {
  it('uses a plain-text error body from the API', () => {
    const error = new AxiosError('Request failed with status code 502', 'ERR_BAD_RESPONSE');
    error.response = responseWith(502, 'Druid query failed: Predicate does not support ARRAY types');

    expect(getErrorMessage(error)).toBe('Druid query failed: Predicate does not support ARRAY types');
  });

  it('serialises a JSON error body', () => {
    const error = new AxiosError('Request failed with status code 400', 'ERR_BAD_REQUEST');
    error.response = responseWith(400, { msg: 'bad order' });

    expect(getErrorMessage(error)).toBe('{"msg":"bad order"}');
  });

  it("falls back to axios's message when there is no response body", () => {
    expect(getErrorMessage(new AxiosError('Network Error', 'ERR_NETWORK'))).toBe('Network Error');
  });
});

describe('HTTPUtils error handling', () => {
  beforeEach(() => {
    useErrorStore.getState().clearErrors();
  });

  it('records a request that failed without a response, instead of dropping it', async () => {
    const response = await HTTPUtils.post(
      'events/timeseries',
      {},
      {
        adapter: () => Promise.reject(new AxiosError('Network Error', 'ERR_NETWORK')),
      }
    );

    expect(response).toMatchObject({ ok: false });
    expect([...useErrorStore.getState().errors]).toEqual(['Network Error']);
  });

  it('records the error body of a failed response', async () => {
    await HTTPUtils.post(
      'events/timeseries',
      {},
      {
        adapter: (config) => {
          const error = new AxiosError('Request failed with status code 502', 'ERR_BAD_RESPONSE', config);
          error.response = responseWith(502, 'Druid query failed: boom');
          return Promise.reject(error);
        },
      }
    );

    expect([...useErrorStore.getState().errors]).toEqual(['Druid query failed: boom']);
  });
});
