import * as React from 'react';
import { Button, Tooltip, message } from 'antd';
import { CloudUploadOutlined } from '@ant-design/icons';

import { requestDeploy } from '../../actions/RulesActions';
import useApplicationConfigStore from '../../stores/ApplicationConfigStore';
import { RuleDeployment, RuleDraftSummary, RuleRecord } from '../../types/RulesTypes';

import { DeployModal } from './DeployModal';

interface DeployButtonProps {
  // The summary, not the full record: this reads `id`, `path`, `rule_name` and `status`
  // and nothing else. Typing it to the narrower shape is what lets one control serve both
  // hosts — the drafts list, which no longer carries SML, and the editor, which holds a
  // full `RuleRecord` that structurally satisfies this.
  //
  // Null when there is nothing saved to act on yet — a new rule, or a `?path=` edit that
  // has never been staged. The control still renders, disabled with a reason, so the
  // header keeps its shape instead of growing a button the first time you save.
  draft: RuleDraftSummary | null;
  onDeployed?: (deployment: RuleDeployment) => void;
  // Fires when a draft is marked as awaiting deployment, so the host can pick up the new
  // status. Separate from `onDeployed` because nothing was deployed — the row moved state
  // and that is all.
  onRequested?: (rule: RuleRecord) => void;
  size?: 'small' | 'middle';
  // A host-supplied reason this particular draft can't be acted on right now — the editor
  // uses it for unsaved changes. Ranks below the ability check: someone who may not deploy
  // at all doesn't need to hear about their unsaved edits first.
  disabledReason?: string;
}

/**
 * Which action this user gets for a draft, and whether they can take it right now.
 *
 * Gating only — the deploy confirmation and everything it needs live in
 * `DeployModal`, which is mounted just while open so that opening is its own reset.
 *
 * The two config flags gate differently on purpose. `ruleDeploymentEnabled` false means
 * the deployment has no rules directory and therefore no deploy story at all, so the
 * control is absent rather than broken. `canDeployRules` false means the deployment does
 * deploy and this user may not — which gets them the action they *can* take rather than a
 * disabled version of one they can't, because that is a standing property of the person
 * and a greyed-out Deploy would never become enabled for them.
 */
export const DeployButton: React.FC<DeployButtonProps> = ({
  draft,
  onDeployed,
  onRequested,
  size = 'small',
  disabledReason,
}) => {
  const ruleDeploymentEnabled = useApplicationConfigStore((state) => state.ruleDeploymentEnabled);
  const canDeployRules = useApplicationConfigStore((state) => state.canDeployRules);
  const [isOpen, setIsOpen] = React.useState<boolean>(false);

  if (!ruleDeploymentEnabled) return null;

  if (!canDeployRules) {
    return <RequestDeployButton draft={draft} size={size} disabledReason={disabledReason} onRequested={onRequested} />;
  }

  // Nothing saved yet, or saved but since edited. Both mean there is no stored text this
  // could act on, so both disable rather than hide — the control keeps its place in the
  // header and says what would make it usable.
  const unavailable = draft == null ? 'Save this draft before deploying it.' : disabledReason;

  if (unavailable != null || draft == null) {
    return (
      <Tooltip title={unavailable}>
        <Button size={size} icon={<CloudUploadOutlined />} disabled>
          Deploy
        </Button>
      </Tooltip>
    );
  }

  const label = draft.status === 'deployed' ? 'Redeploy' : 'Deploy';

  return (
    <>
      {/* Deliberately not `type="primary" ghost`. Ghost paints the primary colour as ink
          rather than as fill, and #404ec1 on the near-black page measures 2.77:1 — under
          WCAG AA for text (4.5:1) and even for a UI boundary (3:1). The default outlined
          button uses the normal foreground colour instead, at 13.4:1.

          Quieter than Save is also the right hierarchy: saving is the routine action,
          deploying is the deliberate one, and a consequential control should not be the
          easiest thing on the row to hit by reflex. */}
      <Button size={size} icon={<CloudUploadOutlined />} onClick={() => setIsOpen(true)}>
        {label}
      </Button>

      {/* Mounted only while open, so every piece of the dialog's state is initialised
          rather than reset. Unmounting is what discards a dismissed attempt. */}
      {isOpen && (
        <DeployModal
          draft={draft}
          label={label}
          onClose={() => setIsOpen(false)}
          onDeployed={(deployment) => {
            setIsOpen(false);
            onDeployed?.(deployment);
          }}
        />
      )}
    </>
  );
};

/**
 * What an author who cannot deploy gets in place of the Deploy button: a way to say the
 * draft is finished and hand it to someone who can ship it.
 *
 * No confirmation dialog and no type-to-confirm. Requesting is reversible, changes
 * nothing live, and is idempotent server-side — the friction that guards a deploy would
 * only be ceremony here. The `summary` field on the draft is the accompanying note, which
 * is why the editor labels it "What's changing? (for the developer)".
 */
const RequestDeployButton: React.FC<{
  draft: RuleDraftSummary | null;
  size: 'small' | 'middle';
  disabledReason?: string;
  onRequested?: (rule: RuleRecord) => void;
}> = ({ draft, size, disabledReason, onRequested }) => {
  const [isRequesting, setIsRequesting] = React.useState<boolean>(false);
  const alreadyRequested = draft?.status === 'deploy_requested';

  // Unsaved changes block a request for the same reason they block a deploy: the request
  // is for whatever text is stored, and editing afterwards resets the row to `draft`
  // server-side — so requesting now would be undone by the next save. Nothing saved at
  // all is the same problem one step earlier.
  const blockedBy = draft == null ? 'Save this draft before requesting deployment.' : disabledReason;

  if (blockedBy != null || draft == null) {
    return (
      <Tooltip title={blockedBy}>
        <Button size={size} icon={<CloudUploadOutlined />} disabled>
          Request deployment
        </Button>
      </Tooltip>
    );
  }

  const onClick = async () => {
    setIsRequesting(true);
    try {
      const rule = await requestDeploy(draft.id);
      message.success('Marked as ready. Someone who can deploy will pick it up from the Rules page.');
      onRequested?.(rule);
    } catch (e) {
      message.error(e instanceof Error ? e.message : String(e));
    } finally {
      setIsRequesting(false);
    }
  };

  return (
    <Tooltip
      title={
        alreadyRequested
          ? 'Already marked as ready. Requesting again changes nothing; editing the draft returns it to a draft.'
          : 'You do not hold CAN_DEPLOY_RULES, so this hands the draft to someone who does.'
      }
    >
      <Button size={size} icon={<CloudUploadOutlined />} loading={isRequesting} onClick={onClick}>
        {alreadyRequested ? 'Deployment requested' : 'Request deployment'}
      </Button>
    </Tooltip>
  );
};
