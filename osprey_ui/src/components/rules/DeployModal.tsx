import * as React from 'react';
import { Alert, Checkbox, Input, Modal, Space, Typography, message } from 'antd';

import { deployRuleDraft, getDeployPlan } from '../../actions/RulesActions';
import {
  MainSmlPlanState,
  RuleDeployPlan,
  RuleDeployment,
  RuleDraftSummary,
  RuleDraftValidationMessage,
  RuleFilePlanState,
} from '../../types/RulesTypes';

import styles from './DeployModal.module.css';

const { Text } = Typography;

// The plan resolves in a few milliseconds against a local rules directory, which makes a
// deploy dialog that snaps fully-formed into place the instant it opens — and something
// that appears instantly reads as a formality rather than a check. Holding the loading
// state for a beat is deliberate friction, of a piece with typing the rule name to
// confirm: this is the one irreversible action in the editor, and it should feel
// considered rather than quick.
//
// It also gates the confirm button, which stays disabled while the plan is loading — so
// the beat is not merely decorative, it is a moment where deploying is genuinely not yet
// possible.
//
// Thirteen 60ms units. The page's transitions are 180ms — three of the same unit — so the
// pause reads as part of the interface's rhythm rather than an arbitrary wait; keep it on
// that grid if you change it.
//
// The duration is chosen to sit in the band between the ~100ms that reads as instantaneous
// and the ~1s where an interface starts to feel broken: long enough to register as a
// deliberate pause, short enough that nobody wonders whether it has hung. Nothing else
// depends on the value.
const PLAN_MIN_LOADING_MS = 780;

// Effect copy, one entry per state the server can report.
const RULE_FILE_EFFECT: Record<RuleFilePlanState, string> = {
  new: 'new file',
  identical: 'identical — nothing changes',
  differs: 'overwrites what is on disk',
};

const MAIN_SML_EFFECT: Record<MainSmlPlanState, string> = {
  would_append: 'append Require(…)',
  already_required: 'already required — nothing changes',
  missing: 'missing from the rules directory',
  unparseable: 'could not be parsed',
};

const CHECKING = 'checking…';

interface DeployModalProps {
  // Non-null: the modal is only mounted once there is something to act on, which is what
  // lets every piece of state below be initialised rather than reset.
  draft: RuleDraftSummary;
  label: string;
  onClose: () => void;
  onDeployed?: (deployment: RuleDeployment) => void;
}

/**
 * The deploy confirmation, and everything only it needs: the plan, the wiring choice, the
 * type-to-confirm gate, and the in-flight state of the deploy itself.
 *
 * Mounted only while open — the caller renders it conditionally — so opening *is* the
 * reset. That replaces a block of `setX(initial)` calls that previously ran on every open
 * to undo the last one, and it means a dismissed attempt cannot leave anything behind.
 */
export const DeployModal: React.FC<DeployModalProps> = ({ draft, label, onClose, onDeployed }) => {
  // Wiring the rule into main.sml is on by default: a deployed file that nothing
  // `Require`s sits on disk inert, so an unwired deploy looks like it worked and changes
  // nothing. Appending is idempotent server-side, so re-deploying an already-wired rule
  // leaves main.sml alone.
  const [wireIntoMain, setWireIntoMain] = React.useState<boolean>(true);
  const [isDeploying, setIsDeploying] = React.useState<boolean>(false);
  // Validation failures are rendered inside the modal rather than in a second modal on
  // top of it: the errors are about the draft you are looking at, and stacking dialogs
  // would hide the thing they describe.
  const [errors, setErrors] = React.useState<RuleDraftValidationMessage[] | null>(null);
  // Typing the rule name to confirm. Deliberate friction: this is the one action in the
  // editor that changes what the engine actually runs, and it is one button press away
  // from a routine save. Making the hand type the name means the deploy can't be the
  // muscle-memory continuation of saving.
  const [confirmText, setConfirmText] = React.useState<string>('');
  // Ties the plain <label> to the input. Generated rather than hardcoded because the
  // control renders once per draft row on the rules page, and duplicate ids would point
  // every label at the first input.
  const confirmInputId = React.useId();
  // What the server says a deploy would actually do. `undefined` while in flight, `null`
  // when the endpoint is unavailable — the plan then falls back to what the API contract
  // guarantees rather than what the disk holds, which is weaker but never wrong.
  const [plan, setPlan] = React.useState<RuleDeployPlan | null | undefined>(undefined);

  React.useEffect(() => {
    let cancelled = false;
    // Derived from the function rather than written as `number`, so this stays correct
    // whichever `setTimeout` is in scope.
    let timer: ReturnType<typeof setTimeout> | undefined;
    const startedAt = Date.now();
    // Held to `PLAN_MIN_LOADING_MS` from the request going out, not added to it: a plan
    // that genuinely takes longer than the floor is shown the moment it lands, so the
    // beat never stacks on top of a slow backend.
    const settle = (next: RuleDeployPlan | null) => {
      timer = setTimeout(
        () => {
          if (!cancelled) setPlan(next);
        },
        Math.max(0, PLAN_MIN_LOADING_MS - (Date.now() - startedAt))
      );
    };
    getDeployPlan(draft.id)
      .then(settle)
      .catch(() => settle(null));
    return () => {
      cancelled = true;
      clearTimeout(timer);
    };
    // Runs once per mount, and a mount is one opening. Not keyed on `wireIntoMain`: the
    // plan answers for both wiring choices, so toggling re-renders from the plan already
    // in hand rather than refetching and flashing a loading state mid-decision.
  }, [draft.id]);

  // Exact match, not trimmed or case-folded: rule names are SML identifiers and are
  // compared exactly everywhere else, so accepting a looser spelling here would teach a
  // looser rule than the one that actually applies. Paste is not blocked: it defeats
  // nothing an attentive person wouldn't also type, and blocking it mainly punishes
  // anyone using a password manager or assistive input.
  const isConfirmed = confirmText === draft.rule_name;

  // `undefined` (still asking) and `null` (nothing to ask) are different answers and must
  // not render alike. Showing the hedged fallback while loading means someone can read
  // "append Require(…) if not already present" and then watch it become "already required
  // — nothing changes"; a plan that rewrites itself under the reader is worse than one
  // that admits it does not know yet.
  //
  // Falling back to the raw state rather than to nothing: these maps are exact-match, so
  // a value this build has not seen would otherwise render an empty cell that looks like
  // the plan has no opinion, which is the one thing it must never look like.
  const isPlanLoading = plan === undefined;

  const ruleFileEffect = isPlanLoading
    ? CHECKING
    : plan
      ? (RULE_FILE_EFFECT[plan.rule_file.state] ?? plan.rule_file.state)
      : 'the rule';

  // Unchecking does not *remove* a Require line, it only declines to add one. So a rule
  // main.sml already requires keeps firing either way, and saying "the rule will not
  // fire" there would be flatly untrue — the one reading that could talk someone out of
  // a deploy they actually wanted.
  const alreadyRequired = plan?.main_sml.state === 'already_required';
  const willFire = wireIntoMain || alreadyRequired;

  // `would_append` shows the literal line the server would write rather than paraphrasing
  // it — the plan can quote itself now that `require_line` is served.
  const mainSmlEffect = !wireIntoMain
    ? alreadyRequired
      ? 'unchanged — already required, so the rule still fires'
      : 'unchanged — the rule will not fire'
    : isPlanLoading
      ? CHECKING
      : plan
        ? plan.main_sml.state === 'would_append' && plan.main_sml.require_line
          ? `append ${plan.main_sml.require_line}`
          : (MAIN_SML_EFFECT[plan.main_sml.state] ?? plan.main_sml.state)
        : 'append Require(…) if not already present';

  // A deploy that would change nothing. Worth saying plainly: it is the one case where
  // the right move is to close the dialog rather than confirm it.
  const isNoOp =
    plan != null &&
    plan.rule_file.state === 'identical' &&
    (!wireIntoMain || plan.main_sml.state === 'already_required');

  // Two independent gates, mirroring the two the plan reports. The checkbox is disabled
  // when main.sml can't take the line; Deploy is disabled when the file can't be written,
  // or when wiring was asked for and isn't possible.
  const wireable = plan == null || plan.wireable_into_main;
  const canDeploy = plan == null || (plan.deployable && (!wireIntoMain || plan.wireable_into_main));

  // Rendered from the same states the plan rows render from, rather than a parallel list
  // of reasons — so an explanation can never disagree with the row above it.
  const problem =
    plan == null
      ? null
      : plan.source.state === 'invalid'
        ? {
            message: 'This draft no longer compiles',
            description:
              'Something it depends on has changed since it was written. Open it in the editor — the validation panel will say what.',
          }
        : wireIntoMain && plan.main_sml.state === 'missing'
          ? {
              message: 'main.sml is missing',
              description:
                'There is no main.sml in the rules directory to require the rule from. Deploy without wiring to write the file anyway.',
            }
          : wireIntoMain && plan.main_sml.state === 'unparseable'
            ? {
                message: 'main.sml does not compile',
                description:
                  'Whether the rule is already required is unanswerable while main.sml is broken, so deploy will not append to it. Deploy without wiring to write the file anyway.',
              }
            : null;

  const onConfirm = async () => {
    setIsDeploying(true);
    setErrors(null);
    try {
      const result = await deployRuleDraft(draft.id, wireIntoMain);
      if (!result.ok) {
        // The stored SML no longer validates against the engine's current sources —
        // something it depended on moved since the draft was written.
        setErrors(result.validation.errors);
        return;
      }
      const { deployment } = result;
      // `main_sml_updated: false` means two opposite things: the rule was already
      // required (fine, it runs), or it was deliberately not wired (it does not run).
      // Reporting both as "main.sml was not changed" hides the only outcome anyone needs
      // to act on, so the wiring choice disambiguates it.
      const where = `Deployed to ${deployment.path_on_disk}.`;
      if (deployment.main_sml_updated) {
        message.success(`${where} Required from main.sml — it is now live.`);
      } else if (wireIntoMain) {
        message.success(`${where} It was already required from main.sml.`);
      } else {
        message.info(`${where} Not required from main.sml, so the rule will not fire yet.`);
      }
      onClose();
      onDeployed?.(deployment);
    } catch (e) {
      message.error(e instanceof Error ? e.message : String(e));
    } finally {
      setIsDeploying(false);
    }
  };

  return (
    <Modal
      open
      title={`${label} ${draft.rule_name}?`}
      okText={label}
      cancelText="Cancel"
      confirmLoading={isDeploying}
      // Blocked or still loading the plan both prevent confirming: deploying against an
      // unknown plan is the thing this dialog exists to stop.
      okButtonProps={{ disabled: !isConfirmed || !canDeploy || isPlanLoading }}
      onOk={onConfirm}
      onCancel={onClose}
      maskClosable={!isDeploying}
    >
      <Space direction="vertical" size={16} style={{ width: '100%' }}>
        {/* One headline, four states, so it can never contradict itself. A separate
            "nothing would change" notice below the plan used to sit under a warning
            about live traffic — both true in their own terms, and incoherent together.
            Severity tracks the actual stakes: no-op is reassuring, unwired is merely
            informational because the file lands but nothing loads it, and only a wiring
            deploy that changes something earns a warning. Loading says so rather than
            guessing, so the banner never flips valence under the reader. */}
        <Alert
          type={isPlanLoading ? 'info' : isNoOp ? 'success' : willFire ? 'warning' : 'info'}
          showIcon
          message={
            isPlanLoading
              ? 'Checking what this would change…'
              : isNoOp
                ? 'Nothing would change'
                : willFire
                  ? `${draft.rule_name} will run against live traffic.`
                  : `Written but not loaded — ${draft.rule_name} will not start firing.`
          }
          description={
            isNoOp
              ? 'What is on disk already matches this draft. Deploying is harmless but pointless — you can close this.'
              : undefined
          }
        />

        <div>
          {/* No spinner on the heading: each row says "checking…" in the slot whose
              value is actually unknown, which is more precise than one indicator over a
              list that is half known already — the paths are certain from the start. */}
          <Text className={styles.planHeading}>What this writes</Text>
          <ul className={styles.planList}>
            <li
              className={`${styles.planItem}${plan?.rule_file.state === 'identical' ? ` ${styles.planItemInactive}` : ''}`}
            >
              <span className={styles.planPath}>{plan?.rule_file.path ?? draft.path}</span>
              <span className={styles.planEffect}>{ruleFileEffect}</span>
            </li>
            {/* The second row is the checkbox's consequence, rendered rather than
                described: toggling below rewrites this line, so the plan can be watched
                changing instead of imagined. */}
            <li
              className={`${styles.planItem}${
                !wireIntoMain || plan?.main_sml.state === 'already_required' ? ` ${styles.planItemInactive}` : ''
              }`}
            >
              <span className={styles.planPath}>main.sml</span>
              <span className={styles.planEffect}>{mainSmlEffect}</span>
            </li>
          </ul>
        </div>

        {problem != null && <Alert type="error" showIcon message={problem.message} description={problem.description} />}

        <div>
          {/* No refetch on change: the plan already answers for both wiring choices, so
              this only re-renders. */}
          <Checkbox
            checked={wireIntoMain}
            disabled={!wireable}
            onChange={(e) => {
              setWireIntoMain(e.target.checked);
            }}
          >
            Require from <code>main.sml</code>
          </Checkbox>
          <p className={styles.checkboxHint}>
            {!wireable
              ? 'Unavailable — main.sml cannot take the line right now.'
              : 'Added only if it isn’t already there, so re-deploying a live rule leaves main.sml alone.'}
          </p>
        </div>

        {/* Deliberately not a `Form.Item`. `App.module.css` uppercases every
            `.ant-form-item-label`, which would render the instruction in capitals while
            the check below compares exactly — telling someone to type a string that would
            be rejected. A plain label sidesteps a global style whose whole purpose is to
            shout, in the one place the text has to be quoted verbatim. */}
        <div>
          <label htmlFor={confirmInputId} className={styles.confirmLabel}>
            Type <code>{draft.rule_name}</code> to confirm
          </label>
          <Input
            id={confirmInputId}
            value={confirmText}
            onChange={(e) => {
              setConfirmText(e.target.value);
            }}
            onPressEnter={() => {
              if (isConfirmed && canDeploy && !isDeploying) void onConfirm();
            }}
            placeholder={draft.rule_name}
            autoComplete="off"
            spellCheck={false}
            // Not autofocused: the modal's job is to make someone read it, and dropping
            // the caret straight into the field invites typing before that.
          />
        </div>

        {errors != null && errors.length > 0 && (
          <Alert
            type="error"
            showIcon
            message="Draft no longer validates, so it was not deployed"
            description={
              <Space direction="vertical" size={4} style={{ width: '100%' }}>
                {errors.map((err, i) => {
                  return (
                    <div key={i}>
                      <div>{err.message}</div>
                      <Text type="secondary" style={{ fontSize: 12 }}>
                        {err.source_path}:{err.line}:{err.column}
                      </Text>
                    </div>
                  );
                })}
              </Space>
            }
          />
        )}
      </Space>
    </Modal>
  );
};
