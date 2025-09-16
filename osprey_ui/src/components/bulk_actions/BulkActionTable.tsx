import React from 'react';

import type { BulkActionJob } from '../../types/BulkActionTypes';

import styles from './BulkActionTable.module.css';
import CopyLinkButton from '../common/CopyLinkButton';
import { Spin } from 'antd';

interface JobTableProps {
  jobs?: BulkActionJob[];
  onCancelJob: (jobId: string) => void;
  jobPollingInProgress: boolean;
}

function isJobInProgress(job: BulkActionJob): boolean {
  return job.status === 'processing' || job.status === 'uploaded' || job.status === 'parsing';
}

export const BulkActionJobTable: React.FC<JobTableProps> = ({ jobs = [], onCancelJob, jobPollingInProgress }) => {
  const formatDate = (dateString: string): string => {
    return new Date(dateString).toLocaleDateString('en-US', {
      year: 'numeric',
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
    });
  };

  const getStatusClass = (status: string): string => {
    switch (status) {
      case 'completed':
        return styles.statusCompleted;
      case 'processing':
      case 'uploaded':
      case 'parsing':
        return styles.statusInProgress;
      case 'failed':
      case 'cancelled':
        return styles.statusFailed;
      default:
        return styles.statusDefault;
    }
  };

  const getStatusDisplay = (job: BulkActionJob) => {
    switch (job.status) {
      case 'completed':
        return 'Completed';
      case 'processing':
        return 'Processing';
      case 'uploaded':
      case 'pending_upload':
        return 'Uploaded & Pending ';
      case 'parsing':
        return 'Setting up job';
      case 'failed':
        return 'Failed';
      case 'cancelled':
        return 'Cancelled';
      default:
        return 'Unknown';
    }
  };

  return (
    <div className={styles.tableContainer}>
      <table className={styles.jobTable}>
        <thead>
          <tr className={styles.tableHeader}>
            <th>Job ID</th>
            <th>Job Name</th>
            <th>Job Description</th>
            <th>Workflow</th>
            <th>Status</th>
            <th>Progress</th>
            <th>Created By</th>
            <th>Created At</th>
            <th>Actions</th>
          </tr>
        </thead>
        <tbody>
          {jobs.map((job: BulkActionJob, index: number) => (
            <tr key={job.id || index} className={`${styles.tableRow} ${isJobInProgress(job) ? styles.inProgress : ''}`}>
              <td className={styles.tableCell}>{job.id}</td>
              <td className={`${styles.tableCell} ${styles.descriptionCell}`}>{job.name}</td>
              <td className={`${styles.tableCell} ${styles.descriptionCell}`}>{job.description}</td>
              <td className={styles.tableCell}>{job.action_workflow_name}</td>
              <td className={styles.tableCell}>
                <span className={`${styles.statusBadge} ${getStatusClass(job.status)}`}>{getStatusDisplay(job)}</span>
                {jobPollingInProgress && isJobInProgress(job) && <Spin size="small" />}
              </td>
              <td className={styles.tableCell}>
                {job.processed_rows ?? 0}/{job.total_rows ?? 0} actions processed
              </td>
              <td className={styles.tableCell}>{job.user_id}</td>
              <td className={styles.tableCell}>{formatDate(job.created_at)}</td>
              <td className={styles.tableCell}>
                <div className={styles.actionsContainer}>
                  <button
                    className={`${styles.actionButton} ${styles.deleteButton} ${
                      job.status === 'completed' || job.status === 'failed' || job.status === 'cancelled'
                        ? styles.disabledButton
                        : ''
                    }`}
                    onClick={() => onCancelJob(job.id)}
                    disabled={job.status === 'completed' || job.status === 'failed' || job.status === 'cancelled'}
                  >
                    Cancel
                  </button>
                </div>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
};
