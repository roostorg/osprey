import create from 'zustand';
import { BulkActionJob, StartBulkActionRequest, StartBulkActionResponse } from '../types/BulkActionTypes';
import { startBulkAction, uploadCompleted, uploadFile, getJobs, cancelJob } from '../actions/BulkJobActions';

export interface ClientFileUploadRequest {
  file: File;
}

function isJobInProgress(job: BulkActionJob): boolean {
  return job.status === 'processing' || job.status === 'uploaded' || job.status === 'parsing';
}

export type StartBulkActionJobRequest = StartBulkActionRequest & ClientFileUploadRequest;

interface BulkActionStore {
  startBulkAction: (request: StartBulkActionJobRequest) => Promise<StartBulkActionResponse>;
  getJobs: () => Promise<(() => void) | void>;
  jobs: BulkActionJob[];
  cancelJob: (jobId: string) => Promise<void>;
  jobPollingInProgress: boolean;
}

const useBulkActionStore = create<BulkActionStore>((set, get) => ({
  startBulkAction: async (request: StartBulkActionJobRequest): Promise<StartBulkActionResponse> => {
    const { file, ...startRequest } = request;

    const result = await startBulkAction(startRequest);

    if (result.url == null) {
      throw new Error('Failed to start bulk action, missing upload url: ' + result.id);
    }

    await uploadFile(result.url, request.file);
    await uploadCompleted(result.id);

    return result;
  },
  getJobs: async (): Promise<(() => void) | void> => {
    const result = await getJobs();
    // Sort jobs by created_at in descending order (most recent first)
    const sortedJobs = [...result].sort((a, b) => new Date(b.created_at).getTime() - new Date(a.created_at).getTime());
    set({ jobs: sortedJobs });

    // Check if there are any in-progress jobs
    const hasInProgressJobs = sortedJobs.some(isJobInProgress);

    if (hasInProgressJobs) {
      set({ jobPollingInProgress: true });
      const pollInterval = setInterval(async () => {
        const updatedJobs = await getJobs();
        // Sort updated jobs as well
        const sortedUpdatedJobs = [...updatedJobs].sort(
          (a, b) => new Date(b.created_at).getTime() - new Date(a.created_at).getTime()
        );
        set({ jobs: sortedUpdatedJobs });

        // Stop polling if no more in-progress jobs
        const stillInProgress = sortedUpdatedJobs.some(isJobInProgress);

        if (!stillInProgress) {
          clearInterval(pollInterval);
          set({ jobPollingInProgress: false });
        }
      }, 5000); // Poll every 5 seconds

      // Return cleanup function
      return () => clearInterval(pollInterval);
    } else {
      set({ jobPollingInProgress: false });
    }
  },
  jobs: [],
  jobPollingInProgress: false,
  cancelJob: async (jobId: string): Promise<void> => {
    await cancelJob(jobId);
    // update the job status in the store
    const jobs = get().jobs;
    const job = jobs.find((job) => job.id === jobId);
    if (job) {
      job.status = 'cancelled';
    }
    set({ jobs: jobs });
  },
}));

export default useBulkActionStore;
