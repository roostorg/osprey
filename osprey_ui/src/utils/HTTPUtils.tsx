import axios, { AxiosError, AxiosResponse } from 'axios';

import useErrorStore from '../stores/ErrorStore';

// Auto-detect Codespaces and construct API URL
const getApiBaseUrl = () => {
  if (process.env.REACT_APP_API_BASE_URL) {
    return process.env.REACT_APP_API_BASE_URL;
  }
  // In Codespaces, replace port 5002 with 5004 in the URL
  if (typeof window !== 'undefined' && window.location.hostname.includes('github.dev')) {
    return window.location.origin.replace('-5002.', '-5004.') + '/';
  }
  return 'http://localhost:5004/';
};
const API_BASE_URL = getApiBaseUrl();
const CORS_HEADER = { 'Access-Control-Allow-Origin': '*' };

const axiosInstance = axios.create({
  baseURL: API_BASE_URL,
  headers: CORS_HEADER,
});

export type HTTPResponse = (AxiosResponse & { ok: true }) | { ok: false; error: AxiosError<any> };

const errorHandler = (error: AxiosError<any>): HTTPResponse => {
  if (error.response != null) {
    const { errors } = useErrorStore.getState();
    useErrorStore.setState({ errors: new Set([...errors, error.response.data]) });
  }

  return { ok: false, error };
};

axiosInstance.interceptors.response.use((response) => ({ ...response, ok: true }), errorHandler);

export default axiosInstance;
