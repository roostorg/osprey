export interface StartBulkActionRequest {
  job_name: string;
  job_description: string;
  workflow_name: string;
  file_name: string;
  entity_type: string;
}

export interface StartBulkActionResponse {
  id: string;
  url: string; // upload url for the file
}

export interface BulkActionJob {
  id: string;
  name: string;
  status: string;
  created_at: string;
  updated_at: string;
  description: string;
  entity_type: string;
  action_workflow_name: string;
  total_rows: number;
  processed_rows: number;
  error: string;
  user_id: string;
}
