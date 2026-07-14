import * as React from 'react';

import { createAskStore } from './AskStore';
import { AskEndpointConfig, AskEvidence } from './types';

export type AskStoreApi = ReturnType<typeof createAskStore>;

export interface ModelOption {
  value: string;
  label: string;
}

export interface AskAiPanelProps {
  /** Host-selected endpoint (URL + optional headers/credentials). */
  endpoint: AskEndpointConfig;
  /** Optional model chooser; when omitted no model selector is shown. */
  models?: ModelOption[];
  defaultModel?: string;
  /** Returns a context_ref to ground the next turn. Never trusted for authorization. */
  getContextSnapshot?: () => string | undefined | Promise<string | undefined>;
  /** Optional custom rendering for a single evidence entry (falls back to a read-only card). */
  renderEvidence?: (evidence: AskEvidence) => React.ReactNode;
  /** Optional action controls rendered alongside each evidence entry. */
  evidenceActions?: (evidence: AskEvidence) => React.ReactNode;
  placeholder?: string;
  title?: string;
  className?: string;
  /** Test/advanced seam: supply a store factory. Defaults to createAskStore(). */
  createStore?: () => AskStoreApi;
}
