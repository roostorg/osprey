export interface BulkJobTask {
  created_at: number;
  task_id: string;
  initiated_by: string;
  task_status: string;
  attempts: number;
  updated_at: number;

  label_status: string;
  label_name: string;
  dimension: string;
  entities_collected: number | undefined;
  entities_labeled: number | undefined;
  total_entities_to_label: number | undefined;
  expected_total_entities_to_label: number | undefined;

  query_filter: string | undefined;
  query_start: number;
  query_end: number;
  label_reason: string;

  no_limit: boolean;
}

// A special class for formatting the way the Bulk Job Task will be dispalyed on the bulk jobs page
export class FormattedBulkJobTask {
  task: BulkJobTask;
  formatted_task: Map<string, string>;

  constructor(task: BulkJobTask) {
    this.task = task;
    this.formatted_task = new Map();

    const addItem = (key: string, value: any) => {
      if (value === null) {
        this.formatted_task.set(key, 'None');
        return;
      }
      this.formatted_task.set(key, String(value));
    };

    let remainingEntities: string = 'N/A';
    if (!this.isCollecting() && task.total_entities_to_label != null && task.entities_labeled != null) {
      remainingEntities = (task.total_entities_to_label - task.entities_labeled).toLocaleString('en-US');
    } else if (
      this.isCollecting() &&
      task.expected_total_entities_to_label != null &&
      task.entities_collected != null
    ) {
      remainingEntities = (task.expected_total_entities_to_label - task.entities_collected).toLocaleString('en-US');
    }

    addItem('Task ID', task.task_id);
    addItem('Initiator', task.initiated_by);
    addItem(
      'Status',
      task.task_status +
        (this.isTaskFinal()
          ? ' as of ' + new Date(task.updated_at * 1000)
          : this.getEstimatedTimeRemaining() !== ''
            ? ' (estimated ' +
              this.getEstimatedTimeRemaining() +
              ' ' +
              (this.isCollecting() ? 'until labelling' : 'remaining') +
              ')'
            : ' (estimate pending)') +
        ' (' +
        task.attempts +
        ' total attempt' +
        (task.attempts === 1 ? '' : 's') +
        ')'
    );

    addItem('Label status applied', task.label_status);
    addItem('Label used', task.label_name + ' on ' + task.dimension);

    if (this.isCollecting()) {
      addItem(
        'Entities collected',
        (task.entities_collected === undefined ? 'N/A' : task.entities_collected.toLocaleString('en-US')) +
          (remainingEntities !== '0' && remainingEntities !== 'N/A'
            ? ' (~' + remainingEntities + (!this.isTaskFinal() ? ' remaining)' : ' dropped)')
            : '')
      );
    } else {
      addItem(
        'Entities labelled',
        task.entities_labeled === undefined
          ? 'N/A'
          : task.entities_labeled.toLocaleString('en-US') +
              (remainingEntities !== '0'
                ? ' (' + remainingEntities + (!this.isTaskFinal() ? ' remaining)' : ' dropped)')
                : '')
      );
    }

    addItem('Query filter', task.query_filter);
    addItem('Query range', new Date(task.query_start * 1000) + ' - ' + new Date(task.query_end * 1000));
    addItem('Label reason', task.label_reason);
  }

  isTaskFinal(): boolean {
    return ['FAILED', 'COMPLETE'].includes(this.task.task_status.toUpperCase());
  }

  isCollecting(): boolean {
    return this.task.task_status.toUpperCase() === 'COLLECTING';
  }

  isLabelling(): boolean {
    return this.task.task_status.toUpperCase() === 'LABELLING';
  }

  /**
   * @returns A formatted string with hours, minutes, and seconds remaining on this task (if applicable)
   */
  getEstimatedTimeRemaining(): string {
    let current_count: number;
    let total_count: number;
    const get_or_default = (value: number | undefined, default_value: number): number => {
      if (!value) {
        return default_value;
      } else {
        return value;
      }
    };
    if (this.isCollecting()) {
      current_count = get_or_default(this.task.entities_collected, 0);
      total_count = get_or_default(this.task.expected_total_entities_to_label, 0);
    } else if (this.isLabelling()) {
      current_count = get_or_default(this.task.entities_labeled, 0);
      total_count = get_or_default(this.task.total_entities_to_label, 0);
    } else {
      return '';
    }
    const timeElapsed: number = Date.now() - new Date(this.task.created_at * 1000).valueOf();
    const estimatedFinishDate: Date =
      this.isTaskFinal() || current_count === undefined || total_count === undefined
        ? new Date(this.task.updated_at * 1000)
        : new Date((total_count / current_count - 1) * timeElapsed + Date.now());
    let tempTime = (estimatedFinishDate.valueOf() - Date.now()) / 1000;
    const secRemaining = Math.floor(tempTime % 60);
    tempTime /= 60;
    const minRemaining = Math.floor(tempTime % 60);
    tempTime /= 60;
    const hrsRemaining = Math.floor(tempTime);
    const estimatedTimeRemaining: string = this.isTaskFinal()
      ? ''
      : (
          (hrsRemaining > 0 ? hrsRemaining + 'h ' : '') +
          (minRemaining > 0 ? minRemaining + 'm ' : '') +
          (secRemaining > 0 ? secRemaining + 's ' : minRemaining <= 0 && hrsRemaining <= 0 ? '0s ' : '')
        ).trimEnd();
    return estimatedTimeRemaining;
  }

  getMap(): Map<string, string> {
    return this.formatted_task;
  }

  // Returns a query url that has the same start, end, and filters as the original query
  getQueryUrl(): string {
    return (
      window.location.origin +
      '/?start=' +
      encodeURIComponent(new Date(this.task.query_start * 1000).toISOString()) +
      '&end=' +
      encodeURIComponent(new Date(this.task.query_end * 1000).toISOString()) +
      '&queryFilter=' +
      encodeURIComponent(this.task.query_filter ? this.task.query_filter : '') +
      '&topn=' +
      encodeURIComponent(this.task.dimension)
    );
  }
}
