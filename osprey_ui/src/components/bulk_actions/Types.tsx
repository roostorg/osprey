export interface Job {
  id?: number | string;
  description: string;
  status: 'Completed' | 'In Progress' | 'Failed' | string;
  createdAt: string;
}
