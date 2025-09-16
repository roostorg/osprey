import axios, { AxiosError, AxiosResponse } from 'axios';

import useErrorStore from '../stores/ErrorStore';

const API_BASE_URL = process.env.REACT_APP_API_BASE_URL;
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
