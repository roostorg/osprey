import axios, { AxiosError, AxiosResponse } from 'axios';

import useErrorStore from '../stores/ErrorStore';

const API_BASE_URL = process.env.REACT_APP_API_BASE_URL;
const CORS_HEADER = { 'Access-Control-Allow-Origin': '*' };

const axiosInstance = axios.create({
  baseURL: API_BASE_URL,
  headers: CORS_HEADER,
});

export type HTTPResponse = (AxiosResponse & { ok: true }) | { ok: false; error: AxiosError<unknown> };

// The UI API sends errors as a plain-text body; fall back to axios's own message (e.g. "Network Error") when the
// request failed without a usable body, so a failed query is never shown as an empty result.
export function getErrorMessage(error: AxiosError<unknown>): string {
  const data = error.response?.data;
  if (typeof data === 'string' && data.trim() !== '') return data;
  if (data != null && typeof data === 'object') return JSON.stringify(data);
  return error.message;
}

const errorHandler = (error: AxiosError<unknown>): HTTPResponse => {
  const { errors } = useErrorStore.getState();
  useErrorStore.setState({ errors: new Set([...errors, getErrorMessage(error)]) });

  return { ok: false, error };
};

axiosInstance.interceptors.response.use((response) => ({ ...response, ok: true }), errorHandler);

export default axiosInstance;
