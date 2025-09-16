import HTTPUtils, { HTTPResponse } from '../utils/HTTPUtils';
import { BulkJobTask } from '../types/BulkJobTypes';
import { BulkActionJob, StartBulkActionRequest, StartBulkActionResponse } from '../types/BulkActionTypes';

export async function getBulkJob(taskId: string | undefined): Promise<BulkJobTask[]> {
  // null on page load
  if (taskId == null) {
    return [];
  }

  // valid job number
  if (!isNaN(Number(taskId))) {
    const response: HTTPResponse = await HTTPUtils.get(`/bulk_history/${taskId}`);

    if (response.ok) {
      return response.data;
    }
  }

  return [];
}

// use numJobs in the future to fetch N number of jobs
export async function getLastNBulkJobs(numJobs: number = 25): Promise<BulkJobTask[]> {
  const response: HTTPResponse = await HTTPUtils.get(`/bulk_history`);

  if (!response.ok) {
    return [];
  }

  return response.data;
}

export async function startBulkAction(request: StartBulkActionRequest): Promise<StartBulkActionResponse> {
  const response: HTTPResponse = await HTTPUtils.post('/bulk_action/start', request);

  if (response.ok) {
    return response.data;
  }

  throw new Error('Failed to start bulk action: ' + response.error);
}

export async function uploadFile(url: string, file: File): Promise<void> {
  const formData = new FormData();
  formData.append('file', file);

  const response: HTTPResponse = await HTTPUtils.post(url, formData);

  if (!response.ok) {
    throw new Error('Failed to upload file: ' + response.error);
  }
}

export async function uploadCompleted(jobId: string): Promise<void> {
  const request = { job_id: jobId };

  const response: HTTPResponse = await HTTPUtils.post(`/bulk_action/upload_completed`, request);

  if (!response.ok) {
    throw new Error('Failed to complete bulk action: ' + response.error);
  }
}

export async function getJobs(): Promise<BulkActionJob[]> {
  const response: HTTPResponse = await HTTPUtils.get(`/bulk_action/jobs`);

  if (!response.ok) {
    return [];
  }

  return response.data.jobs;
}

export async function cancelJob(jobId: string): Promise<void> {
  const response: HTTPResponse = await HTTPUtils.post(`/bulk_action/jobs/${jobId}/cancel`);

  if (!response.ok) {
    throw new Error('Failed to cancel job: ' + response.error);
  }
}
