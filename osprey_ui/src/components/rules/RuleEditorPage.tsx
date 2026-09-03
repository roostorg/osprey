import * as React from 'react';
import {
  Alert,
  Button,
  Card,
  Descriptions,
  Form,
  Input,
  Segmented,
  Select,
  Space,
  Tag,
  Tooltip,
  Typography,
  message,
} from 'antd';
import { DeleteOutlined, PlusOutlined, SaveOutlined } from '@ant-design/icons';
import { Link, useHistory, useLocation } from 'react-router-dom';

import {
  createRuleDraft,
  getRuleDraft,
  getRuleDrafts,
  getRuleSource,
  getRuleVocabulary,
  parseRuleDraftIntoBuilder,
  validateRuleDraft,
} from '../../actions/RulesActions';
import usePromiseResult from '../../hooks/usePromiseResult';
import useApplicationConfigStore from '../../stores/ApplicationConfigStore';
import {
  ParseIntoBuilderResponse,
  RuleDraftSummary,
  RuleDraftValidationMessage,
  RuleDraftValidationResponse,
  RuleRecord,
  RuleVocabulary,
} from '../../types/RulesTypes';
import { renderFromPromiseResult } from '../../utils/PromiseResultUtils';

import { DeployButton } from './DeployButton';

import {
  CONDITION_OPERATOR_OPTIONS,
  Condition,
  ConditionOperator,
  EMPTY_BUILDER_MODEL,
  Outcome,
  OutcomeArg,
  RuleBuilderModel,
  SML_IDENTIFIER_RE,
  applyMissingImports,
  generateSmlFromBuilder,
  outcomeArgsForEffect,
} from './ruleBuilderSml';

import styles from './RuleEditorPage.module.css';

const { Title, Text, Paragraph } = Typography;

// Two independent axes. `EditorView` is what the page is for right now — changing the
// rule, or reading it back. `EditorMode` is which of the two editing surfaces you use to
// do the changing, and only means anything inside the Edit view.
type EditorView = 'edit' | 'preview';
type EditorMode = 'builder' | 'code';

// Saving stages the rule in the rules table for review. Deploying is a separate,
// separately-permissioned step — see `DeployButton` — so saving never touches a
// live rule whatever abilities the author holds.
//
// `rejected` is distinct from `error`: the API refused the save and said why in the same
// structured envelope validation uses (SML that doesn't compile, a rule name another
// draft holds, main.sml as the target), so those render through the validation panel
// instead of as an opaque message.
type SubmitState =
  | { kind: 'idle' }
  | { kind: 'saving' }
  | { kind: 'saved' }
  | { kind: 'rejected'; validation: RuleDraftValidationResponse }
  | { kind: 'error'; message: string };

// Long enough to sit out the pauses inside a sentence and fire between thoughts instead.
// At 600ms an ordinary pause between words tripped it, so a minute of writing produced a
// request per phrase — each one assembling the engine's sources server-side.
const VALIDATE_DEBOUNCE_MS = 1000;

const EDITOR_MODE_STORAGE_KEY = 'osprey-ui-rule-editor-mode';

// Which editor someone last chose, remembered across reloads. Only ever written from an
// explicit switch, never from the default a page happened to open with — otherwise merely
// visiting "Add rule", which starts in the builder, would silently become a preference.
function getStoredEditorMode(): EditorMode | null {
  if (typeof window === 'undefined') return null;
  const stored = window.localStorage.getItem(EDITOR_MODE_STORAGE_KEY);
  return stored === 'builder' || stored === 'code' ? stored : null;
}

interface BootstrapData {
  vocabulary: RuleVocabulary;
  initialSource: string;
  initialPath: string;
  isNewRule: boolean;
  // Every existing draft, used to warn about name/path collisions the server-side
  // validator can't see (it only knows deployed rules, not other drafts). Summaries:
  // only `path` and `rule_name` are read, and the list no longer carries SML.
  existingDrafts: RuleDraftSummary[];
  // The result of round-tripping the loaded source through the backend parser, when it
  // was worth asking. `undefined` means "not checked", not "unsupported" — a new rule
  // never needs checking, and neither does an editor opening in Code, which is why this
  // is absent far more often than it is present.
  initialBuilderParse?: ParseIntoBuilderResponse;
  // The stored row, when this page was opened on an existing draft (`?draftId=`). Set
  // so a deploy can be offered without a redundant save; absent for a new rule and for
  // `?path=` edits, which read a file off disk that has no row behind it yet.
  initialDraft?: RuleRecord;
}

export const RuleEditorPage: React.FC = () => {
  // Two edit entry points, both via query params because react-router v5 has no
  // clean repeating-segment param and rule paths contain slashes:
  //   ?draftId=N  -> edit a saved draft (its SML lives in the rules table)
  //   ?path=X     -> edit an existing rule file loaded from the rules directory
  const history = useHistory();
  const location = useLocation();
  const isNewRule = location.pathname === '/rules/new';
  const params = new URLSearchParams(location.search);
  const draftId = isNewRule ? undefined : (params.get('draftId') ?? undefined);
  const editPath = isNewRule ? undefined : (params.get('path') ?? undefined);
  const canEditRules = useApplicationConfigStore((state) => state.canEditRules);

  const result = usePromiseResult<BootstrapData>(async () => {
    // The parse answers two things: what model the builder starts from, and whether the
    // builder can represent this file at all. Only the first is needed at load, and only
    // when the editor is about to open in the builder — the second can wait until someone
    // asks for the builder, which `onModeChange` already handles by parsing and reporting
    // the reason it can't. Skipping it removes a request, and a *serialised* round trip
    // (it depends on the draft fetch above it), from every code-first editor open.
    const wantsBuilder = getStoredEditorMode() === 'builder';
    const [vocabulary, { drafts: existingDrafts }] = await Promise.all([getRuleVocabulary(), getRuleDrafts()]);
    if (isNewRule) {
      return {
        vocabulary,
        existingDrafts,
        initialSource: '',
        initialPath: 'rules/new_rule.sml',
        isNewRule: true,
      };
    }
    // A draft's SML is in the table, so load it from there rather than from disk.
    if (draftId) {
      // Passed through as the opaque string it arrives as: draft ids are serialised as
      // strings because a 64-bit id would lose its low bits through a JS number.
      const draft = await getRuleDraft(draftId);
      const initialBuilderParse = wantsBuilder ? await parseRuleDraftIntoBuilder(draft.path, draft.source) : undefined;
      return {
        vocabulary,
        existingDrafts,
        initialSource: draft.source,
        initialPath: draft.path,
        isNewRule: false,
        initialBuilderParse,
        initialDraft: draft,
      };
    }
    if (!editPath) {
      throw new Error('Missing ?draftId= or ?path= query parameter; navigate from the Rules page.');
    }

    // A `?path=` editor is identified by file path, not by row — so once it saves, the
    // draft it just created is indistinguishable to it from a stranger's draft at the same
    // path. Reload and it reopens the file, warns about "an edit in progress", and the
    // next save replaces the row it made a moment ago. That loop is reachable by one
    // person doing nothing unusual: edit, save, deploy, edit again.
    //
    // Adopting the existing draft closes it. Same rule the registry's Edit link follows,
    // and the file is not fetched at all in this case because the draft supersedes it.
    const staged = existingDrafts.find((d) => {
      return d.path === editPath && (d.status === 'draft' || d.status === 'deploy_requested');
    });
    if (staged) {
      const draft = await getRuleDraft(staged.id);
      const initialBuilderParse = wantsBuilder ? await parseRuleDraftIntoBuilder(draft.path, draft.source) : undefined;
      return {
        vocabulary,
        existingDrafts,
        initialSource: draft.source,
        initialPath: draft.path,
        isNewRule: false,
        initialBuilderParse,
        initialDraft: draft,
      };
    }

    const source = await getRuleSource(editPath);
    const initialBuilderParse = wantsBuilder
      ? await parseRuleDraftIntoBuilder(source.path, source.contents)
      : undefined;
    return {
      vocabulary,
      existingDrafts,
      initialSource: source.contents,
      initialPath: source.path,
      isNewRule: false,
      initialBuilderParse,
    };
  }, [draftId, editPath, isNewRule]);

  // RulesPage hides its authoring entry points without this ability, but the route is
  // still reachable by URL — a bookmark, or a link shared by someone who does have it.
  // Every request this page makes needs CAN_EDIT_RULES, so say so rather than letting
  // the vocabulary fetch fail and render as a generic load error.
  if (!canEditRules) {
    return (
      <div className={styles.viewContainer}>
        <div className={styles.scrollArea}>
          <Alert
            type="warning"
            showIcon
            message="You can't author rules"
            description="Editing rule drafts needs the CAN_EDIT_RULES ability. Ask an Osprey admin to grant it, or browse the Rules Registry instead."
            action={
              <Button size="small" onClick={() => history.push('/rules')}>
                Back to rules
              </Button>
            }
          />
        </div>
      </div>
    );
  }

  return renderFromPromiseResult(result, (data) => {
    return <RuleEditorView data={data} />;
  });
};

const RuleEditorView: React.FC<{ data: BootstrapData }> = ({ data }) => {
  const history = useHistory();
  const currentUserEmail = useApplicationConfigStore((state) => state.currentUser.email);
  // "Not known to be unsupported", rather than "confirmed supported". The bootstrap only
  // parses when it is about to open the builder, so `undefined` is the common case and
  // must not read as a refusal — it means nobody has asked yet. Someone who does ask goes
  // through `onModeChange`, which parses then and reports the reason if it can't.
  const builderAllowed = data.initialBuilderParse?.supported !== false;
  const builderDisabledReason = data.initialBuilderParse?.supported === false ? data.initialBuilderParse.reason : '';
  // Not persisted, unlike `mode`: arriving on the editor to edit is right every time,
  // where which editing surface you prefer is a lasting preference.
  const [view, setView] = React.useState<EditorView>('edit');
  const [mode, setMode] = React.useState<EditorMode>(() => {
    // A stored preference never overrides `builderAllowed`: the builder is off because
    // this file's SML cannot be represented in it, which no preference can change.
    if (!builderAllowed) return 'code';
    return getStoredEditorMode() ?? (data.isNewRule ? 'builder' : 'code');
  });
  const [path, setPath] = React.useState<string>(data.initialPath);
  const [codeSource, setCodeSource] = React.useState<string>(data.initialSource);
  const [builder, setBuilder] = React.useState<RuleBuilderModel>(() => {
    if (data.initialBuilderParse?.supported === true) {
      return data.initialBuilderParse.model;
    }
    return EMPTY_BUILDER_MODEL;
  });
  // Seeded from the stored draft, not blank: it is a saved field like the path and the
  // source, so opening a draft and re-saving it must not silently erase the note that
  // explains why the rule exists. Blank for a new rule and for `?path=` edits, which have
  // no row behind them yet.
  const [summary, setSummary] = React.useState<string>(data.initialDraft?.summary ?? '');
  // The stored row this editor is currently in sync with, or null when nothing has been
  // saved yet. Deploying writes whatever the *table* holds, not what is in the textarea,
  // so the deploy control is only offered while these agree.
  const [savedDraft, setSavedDraft] = React.useState<RuleRecord | null>(data.initialDraft ?? null);
  const [validation, setValidation] = React.useState<RuleDraftValidationResponse | null>(null);
  const [isValidating, setIsValidating] = React.useState<boolean>(false);
  const [submitState, setSubmitState] = React.useState<SubmitState>({ kind: 'idle' });

  const effectiveSource = mode === 'builder' ? generateSmlFromBuilder(builder, data.vocabulary.features) : codeSource;

  // What this editor rendered when it last agreed with the server, rather than the bytes
  // the server holds. The Builder regenerates its SML from a model — its own indentation,
  // quoting and trailing commas — so it practically never reproduces stored text byte for
  // byte. Comparing against `savedDraft.source` therefore answers "would saving change
  // the bytes?", which is true the instant an existing draft opens in the Builder,
  // untouched. The baseline answers "has anything been changed?", which is the question
  // the Save button is actually asking.
  // State rather than a ref: it is read during render to decide whether Save is enabled,
  // and a ref read in render would not re-render when it changes. The argument is
  // evaluated every render but only used on the first, which is what seeds the baseline
  // from whatever the initial mode rendered.
  //
  // All three saved fields are baselined, not just the source, so dirtiness never depends
  // on whether a stored row happens to exist. A `?path=` edit has no row — it is a file on
  // disk that was never staged — and keying off that made it permanently dirty, lighting
  // up Save about a second after load on a page nobody had touched.
  const [baseline, setBaseline] = React.useState<{ source: string; path: string; summary: string }>({
    source: effectiveSource,
    path: data.initialPath,
    summary: data.initialDraft?.summary ?? '',
  });

  React.useEffect(() => {
    let cancelled = false;
    if (!effectiveSource.trim()) {
      // eslint-disable-next-line react-hooks/set-state-in-effect -- clearing stale validation on input empty
      setValidation(null);
      setIsValidating(false);
      return;
    }
    setIsValidating(true);
    // Aborted by the cleanup below, so a request already in flight when the source
    // changes is dropped at the socket rather than merely having its reply discarded.
    const controller = new AbortController();
    const handle = window.setTimeout(async () => {
      try {
        const result = await validateRuleDraft(path, effectiveSource, controller.signal);
        if (!cancelled) setValidation(result);
      } catch (e) {
        if (!cancelled) {
          setValidation({
            ok: false,
            errors: [
              {
                message: e instanceof Error ? e.message : String(e),
                hint: '',
                source_path: path,
                line: 0,
                column: 0,
                rendered: '',
              },
            ],
            warnings: [],
          });
        }
      } finally {
        if (!cancelled) setIsValidating(false);
      }
    }, VALIDATE_DEBOUNCE_MS);
    return () => {
      cancelled = true;
      window.clearTimeout(handle);
      controller.abort();
    };
  }, [effectiveSource, path]);

  const ruleNameForSubmit = mode === 'builder' ? builder.ruleName : guessRuleNameFromSource(codeSource);

  // Collisions the server-side validator can't see (it only knows deployed rules):
  // a rule name already taken by another draft (a hard conflict — the server rejects
  // it on save too), and — for a brand new rule — a path that would overwrite a live
  // rule or another draft (a soft warning, since replacing can be intentional). Path
  // is only editable for new rules, so path warnings are new-rule-only.
  const nameConflictDraft = data.existingDrafts.find((d) => d.rule_name === ruleNameForSubmit && d.path !== path);
  const pathOverwritesRule = data.isNewRule && data.vocabulary.source_files.includes(path);
  // Another row at this path with work *not yet live* in it. Drafts are upserted by path,
  // so saving here would replace it — and unlike the deployed case that is a real loss,
  // because nothing else holds that text.
  //
  // Deployed rows are deliberately excluded. Editing a live rule and saving is the ordinary
  // flow — the backend returns the row to `draft` for exactly that — so warning about it
  // would cry conflict over the most common thing anyone does here.
  const pathOverwritesDraft = data.existingDrafts.find((d) => {
    return d.path === path && d.id !== savedDraft?.id && (d.status === 'draft' || d.status === 'deploy_requested');
  });

  // Whether anything differs from the row the editor last synced with. Compared field by
  // field against the source we already hold rather than by re-hashing to compare `cid`:
  // same answer, but exact and synchronous, and it catches a changed `path` or `summary`
  // which the content hash by definition does not cover.
  //
  // Always dirty when there is no row yet — a new rule, or a `?path=` edit of a file that
  // exists on disk but has never been staged as a draft.
  const isDirty = effectiveSource !== baseline.source || path !== baseline.path || summary !== baseline.summary;

  const canSave =
    isDirty &&
    !!validation?.ok &&
    SML_IDENTIFIER_RE.test(ruleNameForSubmit) &&
    submitState.kind !== 'saving' &&
    !!effectiveSource.trim() &&
    !nameConflictDraft;

  // The builder and code editor hold independent state, so a tab switch has to
  // carry content across: builder -> code dumps the generated SML into the
  // textarea, code -> builder re-parses the (possibly hand-edited) source so
  // the form never silently submits a stale model.
  // Only switches that actually land are remembered, so a failed parse leaves the stored
  // preference alone rather than recording a builder the user never got to.
  const applyMode = (next: EditorMode) => {
    if (typeof window !== 'undefined') {
      window.localStorage.setItem(EDITOR_MODE_STORAGE_KEY, next);
    }
    setMode(next);
  };

  const onModeChange = async (next: EditorMode) => {
    if (next === mode) return;
    if (next === 'code') {
      setCodeSource(generateSmlFromBuilder(builder, data.vocabulary.features));
      applyMode('code');
      return;
    }
    if (!codeSource.trim()) {
      applyMode('builder');
      return;
    }
    try {
      const parsed = await parseRuleDraftIntoBuilder(path, codeSource);
      if (parsed.supported) {
        setBuilder(parsed.model);
        applyMode('builder');
      } else {
        message.warning(`Rule Builder can't represent this file: ${parsed.reason}. Keep editing in Code Editor.`);
      }
    } catch (e) {
      message.warning(`Could not parse this file for Rule Builder: ${e instanceof Error ? e.message : String(e)}`);
    }
  };

  const onSave = async () => {
    if (!canSave) return;
    setSubmitState({ kind: 'saving' });
    try {
      const result = await createRuleDraft({
        path,
        source: effectiveSource,
        rule_name: ruleNameForSubmit,
        summary,
      });
      if (!result.ok) {
        setSubmitState({ kind: 'rejected', validation: result.validation });
        return;
      }
      setSavedDraft(result.draft);
      // Re-baseline to what was just sent, not to `result.draft.source`: keeping the
      // comparison against what this surface renders is what makes a clean save stay
      // clean in the Builder, where the two are equal in meaning but not in bytes.
      setBaseline({ source: effectiveSource, path, summary });
      setSubmitState({ kind: 'saved' });
      message.success('Draft saved.');
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      setSubmitState({ kind: 'error', message: msg });
    }
  };

  // Deploy acts on the stored row, so anything typed since the last save would be
  // silently left behind. Rather than hide the control — which reads as "you can't
  // deploy" — keep it in place and say what to do about it. Same `isDirty` the Save
  // button uses, so the two controls can never disagree about whether there is work
  // outstanding.
  const deployDisabledReason =
    savedDraft != null && isDirty
      ? 'Save your changes first — deploying writes the last saved draft, not what is in the editor.'
      : undefined;

  // The header's blurb is orientation for someone arriving on the page, and dead weight
  // once they are working — so it collapses on the first scroll, giving the height back
  // to the editor. Driven off the scroll container rather than a media query because it
  // is about where you are in the page, not how big the window is.
  const bodyRef = React.useRef<HTMLDivElement>(null);
  const [isScrolled, setIsScrolled] = React.useState<boolean>(false);

  React.useEffect(() => {
    const el = bodyRef.current;
    if (el == null) return;
    const onScroll = () => {
      // A few pixels of slack so a trackpad's inertial overscroll doesn't flicker the
      // blurb in and out at the very top of the page.
      setIsScrolled(el.scrollTop > 8);
    };
    el.addEventListener('scroll', onScroll, { passive: true });
    return () => {
      el.removeEventListener('scroll', onScroll);
    };
  }, []);

  return (
    <div className={styles.viewContainer}>
      <header className={`${styles.header}${isScrolled ? ` ${styles.headerCompact}` : ''}`}>
        <div className={styles.headerLeft}>
          <Title level={3} style={{ margin: 0 }}>
            {data.isNewRule ? 'Add rule' : 'Edit rule'}
          </Title>
          <Text type="secondary" className={styles.headerBlurb}>
            Stages the rule as a draft for review. Saving never changes a live rule — deploying is a separate step, and
            a separate ability.
          </Text>
        </div>
        <div className={styles.headerActions}>
          {/* Are you changing the rule, or looking at it? Builder-vs-Code sits a level
              down, on the editing card, because both of those are ways of editing. */}
          <Segmented<EditorView>
            value={view}
            onChange={setView}
            options={[
              { label: 'Edit', value: 'edit' },
              { label: 'Preview', value: 'preview' },
            ]}
          />
          <div className={styles.headerButtons}>
            {/* Three weights for three kinds of action: leaving is incidental (text),
                deploying is deliberate (outlined), saving is what you came to do (solid).
                Cancel drops to text so Deploy is the only outlined control and the two
                are not mistaken for each other. */}
            <Button type="text" onClick={() => history.push('/rules')}>
              Cancel
            </Button>
            <Button
              type="primary"
              icon={<SaveOutlined />}
              disabled={!canSave}
              onClick={onSave}
              loading={submitState.kind === 'saving'}
            >
              Save draft
            </Button>

            <DeployButton
              draft={savedDraft}
              size="middle"
              disabledReason={deployDisabledReason}
              onDeployed={(deployment) => {
                setSavedDraft(deployment.rule);
              }}
              onRequested={(rule) => {
                setSavedDraft(rule);
              }}
            />
          </div>
        </div>
      </header>

      <div className={styles.body} ref={bodyRef}>
        <div className={styles.editorColumn}>
          <SubmitBanner submitState={submitState} />

          {nameConflictDraft && (
            <Alert
              type="warning"
              showIcon
              style={{ marginBottom: 12 }}
              message="Rule name already used by another draft"
              description={`The draft at ${nameConflictDraft.path} already defines a rule named "${ruleNameForSubmit}". Rule names must be unique, so saving will be rejected until you rename this rule or edit that draft.`}
            />
          )}
          {pathOverwritesRule && (
            <Alert
              type="warning"
              showIcon
              style={{ marginBottom: 12 }}
              message="A live rule already exists at this path"
              description={`Saving stages a draft that would replace ${path} when deployed. Pick a different path if you meant to add a new rule instead of changing that one.`}
            />
          )}
          {pathOverwritesDraft && (
            <Alert
              type="warning"
              showIcon
              style={{ marginBottom: 12 }}
              message={
                pathOverwritesDraft.author === currentUserEmail
                  ? 'You already have an edit in progress for this rule'
                  : 'Someone is already editing this rule'
              }
              description={
                <Space direction="vertical" size={4} style={{ width: '100%' }}>
                  {/* Naming the current user back to themselves reads as a stranger; the
                      advice also differs, since losing your own unsaved work is a mistake
                      rather than a collision to coordinate around. */}
                  <span>
                    {pathOverwritesDraft.author === currentUserEmail ? (
                      <>
                        There is already a draft at <code>{path}</code>. Drafts are stored by path, so saving here
                        replaces it — the work in that draft would be lost.
                      </>
                    ) : (
                      <>
                        {pathOverwritesDraft.author} has an edit in progress at <code>{path}</code>. Drafts are stored
                        by path, so saving here replaces theirs — their changes would be lost.
                      </>
                    )}
                  </span>
                  <Link
                    to={{ pathname: '/rules/edit', search: `?draftId=${encodeURIComponent(pathOverwritesDraft.id)}` }}
                  >
                    Open that draft instead
                  </Link>
                </Space>
              }
            />
          )}

          {view === 'preview' ? (
            <DraftPreview
              path={path}
              summary={summary}
              source={effectiveSource}
              isNewRule={data.isNewRule}
              savedDraft={savedDraft}
              isDirty={isDirty}
            />
          ) : (
            <>
              <Card size="small" style={{ marginBottom: 12 }}>
                <Form layout="vertical" size="small">
                  <Form.Item label="File path" tooltip="Path inside the rules directory where this file will live.">
                    <Input value={path} onChange={(e) => setPath(e.target.value)} disabled={!data.isNewRule} />
                  </Form.Item>
                  <Form.Item
                    label={
                      data.isNewRule ? 'Why this rule? (for the developer)' : "What's changing? (for the developer)"
                    }
                    tooltip="Saved alongside the draft to give the developer who deploys it context. Not written into the rule file itself."
                    style={{ marginBottom: 0 }}
                  >
                    <Input.TextArea
                      value={summary}
                      onChange={(e) => setSummary(e.target.value)}
                      placeholder={
                        data.isNewRule
                          ? 'Why do we need this rule? What behaviour does it target?'
                          : 'What are you changing about this rule, and why?'
                      }
                      autoSize={{ minRows: 2, maxRows: 4 }}
                    />
                  </Form.Item>
                </Form>
              </Card>

              <EditSurface
                mode={mode}
                onModeChange={onModeChange}
                builderAllowed={builderAllowed}
                builderDisabledReason={builderDisabledReason}
                builder={builder}
                setBuilder={setBuilder}
                vocabulary={data.vocabulary}
                codeSource={codeSource}
                setCodeSource={setCodeSource}
                validation={validation}
              />
            </>
          )}
        </div>

        <aside className={styles.sidePanel}>
          <ValidationPanel validation={validation} isValidating={isValidating} />
          <VocabularyPanel vocabulary={data.vocabulary} />
        </aside>
      </div>
    </div>
  );
};

// The editing surface: one card, two interchangeable ways of authoring the same SML. The
// Builder/Code choice lives on the card rather than in the page header because both are
// editing — the header's axis is whether you are editing at all.
const EditSurface: React.FC<{
  mode: EditorMode;
  onModeChange: (next: EditorMode) => void;
  builderAllowed: boolean;
  builderDisabledReason: string;
  builder: RuleBuilderModel;
  setBuilder: React.Dispatch<React.SetStateAction<RuleBuilderModel>>;
  vocabulary: RuleVocabulary;
  codeSource: string;
  setCodeSource: React.Dispatch<React.SetStateAction<string>>;
  validation: RuleDraftValidationResponse | null;
}> = ({
  mode,
  onModeChange,
  builderAllowed,
  builderDisabledReason,
  builder,
  setBuilder,
  vocabulary,
  codeSource,
  setCodeSource,
  validation,
}) => {
  return (
    <Card
      size="small"
      title={mode === 'builder' ? 'Rule' : 'SML source'}
      extra={
        <Tooltip
          title={
            builderAllowed ? '' : `Rule Builder can't represent this file: ${builderDisabledReason}. Edit as code.`
          }
        >
          <Segmented<EditorMode>
            size="small"
            value={mode}
            onChange={onModeChange}
            options={[
              { label: 'Builder', value: 'builder', disabled: !builderAllowed },
              { label: 'Code', value: 'code' },
            ]}
          />
        </Tooltip>
      }
    >
      {mode === 'code' && validation?.suggested_imports && validation.suggested_imports.length > 0 && (
        <Alert
          type="warning"
          showIcon
          style={{ marginBottom: 12 }}
          message="Missing imports"
          description={
            <Space direction="vertical" size={4} style={{ width: '100%' }}>
              <span>This file references identifiers defined in: {validation.suggested_imports.join(', ')}</span>
              <Button
                size="small"
                type="primary"
                onClick={() => {
                  return setCodeSource((prev) => {
                    return applyMissingImports(prev, validation.suggested_imports ?? []);
                  });
                }}
              >
                Add missing imports
              </Button>
            </Space>
          }
        />
      )}

      {mode === 'builder' ? (
        <RuleBuilderEditor model={builder} setModel={setBuilder} vocabulary={vocabulary} />
      ) : (
        <Input.TextArea
          className={styles.codeArea}
          value={codeSource}
          onChange={(e) => setCodeSource(e.target.value)}
          placeholder="MyRule = Rule(when_all=[PostText == 'hello'], description='...')"
          autoSize={{ minRows: 24, maxRows: 60 }}
          spellCheck={false}
        />
      )}
    </Card>
  );
};

// The draft exactly as it will be stored: the metadata alongside the SML, read-only. Not
// the same thing as the Code editor even in code mode — that shows only the SML, and
// shows it editable.
const DraftPreview: React.FC<{
  path: string;
  summary: string;
  source: string;
  isNewRule: boolean;
  savedDraft: RuleRecord | null;
  isDirty: boolean;
}> = ({ path, summary, source, isNewRule, savedDraft, isDirty }) => {
  return (
    <Card size="small" title="Draft preview">
      {savedDraft != null && isDirty && (
        <Alert
          type="info"
          showIcon
          style={{ marginBottom: 12 }}
          message="Showing your unsaved changes"
          description="This is what saving would store. The version currently in the rules table is the one before your edits."
        />
      )}
      <Descriptions size="small" column={1} colon={false} styles={{ label: { width: 150, fontWeight: 600 } }}>
        <Descriptions.Item label="File path">
          <code>{path}</code>
        </Descriptions.Item>
        <Descriptions.Item label={isNewRule ? 'Why this rule?' : "What's changing?"}>
          {summary ? <Text>{summary}</Text> : <Text style={{ fontStyle: 'italic' }}>not given</Text>}
        </Descriptions.Item>
      </Descriptions>
      <pre className={styles.previewBlock}>{source}</pre>
    </Card>
  );
};

const SubmitBanner: React.FC<{ submitState: SubmitState }> = ({ submitState }) => {
  if (submitState.kind === 'idle') return null;
  if (submitState.kind === 'saving') {
    return <Alert type="info" message="Saving draft..." showIcon style={{ marginBottom: 12 }} />;
  }
  if (submitState.kind === 'saved') {
    return (
      <Alert
        type="success"
        showIcon
        style={{ marginBottom: 12 }}
        message="Draft saved"
        description="Staged in the rules table for review. No live rules changed until someone deploys it."
      />
    );
  }
  if (submitState.kind === 'rejected') {
    return (
      <Alert
        type="error"
        showIcon
        style={{ marginBottom: 12 }}
        message="Draft not saved"
        description={
          <Space direction="vertical" size={4} style={{ width: '100%' }}>
            {submitState.validation.errors.map((err, i) => {
              return <ValidationMessageRow key={`reject-${i}`} kind="error" msg={err} />;
            })}
          </Space>
        }
      />
    );
  }
  return (
    <Alert type="error" showIcon style={{ marginBottom: 12 }} message="Failed" description={submitState.message} />
  );
};

const ValidationPanel: React.FC<{
  validation: RuleDraftValidationResponse | null;
  isValidating: boolean;
}> = ({ validation, isValidating }) => {
  return (
    <Card
      size="small"
      title={
        <Space>
          <span>Validation</span>
          {isValidating && <Tag>checking…</Tag>}
          {!isValidating && validation?.ok === true && <Tag color="green">valid</Tag>}
          {!isValidating && validation?.ok === false && <Tag color="red">errors</Tag>}
        </Space>
      }
    >
      {!validation && <Text type="secondary">Start typing to see live validation against the engine.</Text>}
      {validation?.assemble_error && (
        <Alert
          type="error"
          showIcon
          style={{ marginBottom: 8 }}
          message="The engine's rules could not be assembled"
          description="Nothing you write here will validate until that is fixed — it is a problem with the deployed rules, not with this draft."
        />
      )}
      {validation?.ok === true && validation.warnings.length === 0 && (
        <Text type="success">Engine accepts this draft.</Text>
      )}
      {validation?.errors && validation.errors.length > 0 && (
        <div className={styles.errorList}>
          {validation.errors.map((err, i) => {
            return <ValidationMessageRow key={`err-${i}`} kind="error" msg={err} />;
          })}
        </div>
      )}
      {validation?.warnings && validation.warnings.length > 0 && (
        <div className={styles.errorList} style={{ marginTop: 8 }}>
          {validation.warnings.map((w, i) => {
            return <ValidationMessageRow key={`warn-${i}`} kind="warning" msg={w} />;
          })}
        </div>
      )}
    </Card>
  );
};

const ValidationMessageRow: React.FC<{ kind: 'error' | 'warning'; msg: RuleDraftValidationMessage }> = ({
  kind,
  msg,
}) => {
  return (
    <div className={kind === 'error' ? styles.errorItem : styles.warningItem}>
      <div style={{ fontWeight: 600 }}>{msg.message}</div>
      {msg.hint && (
        <div style={{ fontSize: 12, marginTop: 2 }}>
          <Text type="secondary">{msg.hint}</Text>
        </div>
      )}
      <div className={styles.errorLocation}>
        {msg.source_path}:{msg.line}:{msg.column}
      </div>
    </div>
  );
};

const VocabularyPanel: React.FC<{ vocabulary: RuleVocabulary }> = ({ vocabulary }) => {
  return (
    <Card size="small" title="Available in rules">
      <Paragraph type="secondary" style={{ fontSize: 12, marginBottom: 6 }}>
        Variables you can reference inside conditions.
      </Paragraph>
      <Space size={4} wrap>
        {vocabulary.features.slice(0, 60).map((f) => {
          return (
            <Tag key={f.name} style={{ fontFamily: 'monospace' }}>
              {f.name}
            </Tag>
          );
        })}
        {vocabulary.features.length > 60 && <Text type="secondary">+{vocabulary.features.length - 60} more</Text>}
      </Space>
      {vocabulary.effects.length > 0 && (
        <>
          <Paragraph type="secondary" style={{ fontSize: 12, marginTop: 12, marginBottom: 6 }}>
            Effects used in existing rules.
          </Paragraph>
          <Space size={4} wrap>
            {vocabulary.effects.map((name) => {
              return (
                <Tag key={name} color="blue" style={{ fontFamily: 'monospace' }}>
                  {name}
                </Tag>
              );
            })}
          </Space>
        </>
      )}
    </Card>
  );
};

// The builder form itself. Renders no card of its own — `EditSurface` owns the card and
// the Builder/Code switch, so this and the code textarea drop into the same frame.
const RuleBuilderEditor: React.FC<{
  model: RuleBuilderModel;
  setModel: React.Dispatch<React.SetStateAction<RuleBuilderModel>>;
  vocabulary: RuleVocabulary;
}> = ({ model, setModel, vocabulary }) => {
  const featureOptions = React.useMemo(() => {
    return vocabulary.features.map((f) => {
      return { label: f.name, value: f.name };
    });
  }, [vocabulary.features]);

  const effectOptions = React.useMemo(() => {
    return vocabulary.effects.map((name) => {
      return { label: name, value: name };
    });
  }, [vocabulary.effects]);

  const updateCondition = (idx: number, patch: Partial<Condition>) => {
    setModel((prev) => {
      const next = [...prev.conditions];
      next[idx] = { ...next[idx], ...patch };
      return { ...prev, conditions: next };
    });
  };
  const addCondition = () => {
    setModel((prev) => ({
      ...prev,
      conditions: [...prev.conditions, { feature: '', operator: '==', rhs: '', rhsIsFeature: false }],
    }));
  };
  const removeCondition = (idx: number) => {
    setModel((prev) => ({
      ...prev,
      conditions: prev.conditions.filter((_, i) => {
        return i !== idx;
      }),
    }));
  };

  const updateOutcome = (idx: number, patch: Partial<Outcome>) => {
    setModel((prev) => {
      const next = [...prev.outcomes];
      next[idx] = { ...next[idx], ...patch };
      return { ...prev, outcomes: next };
    });
  };
  const updateOutcomeArg = (oIdx: number, aIdx: number, patch: Partial<OutcomeArg>) => {
    setModel((prev) => {
      const outcomes = [...prev.outcomes];
      const args = [...outcomes[oIdx].args];
      args[aIdx] = { ...args[aIdx], ...patch };
      outcomes[oIdx] = { ...outcomes[oIdx], args };
      return { ...prev, outcomes };
    });
  };
  const addOutcome = () => {
    setModel((prev) => ({ ...prev, outcomes: [...prev.outcomes, { effect: '', args: [] }] }));
  };
  const removeOutcome = (idx: number) => {
    setModel((prev) => ({
      ...prev,
      outcomes: prev.outcomes.filter((_, i) => {
        return i !== idx;
      }),
    }));
  };

  return (
    <RuleBuilderForm
      model={model}
      setModel={setModel}
      featureOptions={featureOptions}
      effectOptions={effectOptions}
      vocabulary={vocabulary}
      updateCondition={updateCondition}
      addCondition={addCondition}
      removeCondition={removeCondition}
      updateOutcome={updateOutcome}
      updateOutcomeArg={updateOutcomeArg}
      addOutcome={addOutcome}
      removeOutcome={removeOutcome}
    />
  );
};

const RuleBuilderForm: React.FC<{
  model: RuleBuilderModel;
  setModel: React.Dispatch<React.SetStateAction<RuleBuilderModel>>;
  featureOptions: { label: string; value: string }[];
  effectOptions: { label: string; value: string }[];
  vocabulary: RuleVocabulary;
  updateCondition: (idx: number, patch: Partial<Condition>) => void;
  addCondition: () => void;
  removeCondition: (idx: number) => void;
  updateOutcome: (idx: number, patch: Partial<Outcome>) => void;
  updateOutcomeArg: (oIdx: number, aIdx: number, patch: Partial<OutcomeArg>) => void;
  addOutcome: () => void;
  removeOutcome: (idx: number) => void;
}> = ({
  model,
  setModel,
  featureOptions,
  effectOptions,
  vocabulary,
  updateCondition,
  addCondition,
  removeCondition,
  updateOutcome,
  updateOutcomeArg,
  addOutcome,
  removeOutcome,
}) => {
  return (
    <>
      <Form layout="vertical">
        <Form.Item
          label="Rule name"
          tooltip="An SML identifier. This becomes the left-hand side of the Rule(...) assignment."
          validateStatus={model.ruleName && !SML_IDENTIFIER_RE.test(model.ruleName) ? 'error' : ''}
          help={
            model.ruleName && !SML_IDENTIFIER_RE.test(model.ruleName)
              ? 'Must be an SML identifier: letters, digits, and underscores, not starting with a digit.'
              : undefined
          }
        >
          <Input
            value={model.ruleName}
            onChange={(e) => setModel((prev) => ({ ...prev, ruleName: e.target.value }))}
            placeholder="ContainsHello"
          />
        </Form.Item>
        <Form.Item
          label="Rule description"
          tooltip="Saved into the rule file as `description='...'`. Shown in the Rules Registry."
        >
          <Input.TextArea
            value={model.description}
            onChange={(e) => setModel((prev) => ({ ...prev, description: e.target.value }))}
            autoSize={{ minRows: 1, maxRows: 3 }}
            placeholder="What does the rule detect?"
          />
        </Form.Item>
      </Form>

      <div className={styles.builderSection}>
        <Title level={5}>Conditions</Title>
        <Paragraph type="secondary" style={{ fontSize: 12 }}>
          Every row must be true for the rule to fire. SML&apos;s <code>when_all</code> is AND-only. For OR, write the
          extra rule in Code Editor.
        </Paragraph>
        {model.conditions.map((cond, idx) => {
          return (
            <div key={idx} className={styles.builderRow}>
              <Select
                showSearch
                placeholder="Variable"
                value={cond.feature || undefined}
                onChange={(value) => updateCondition(idx, { feature: value })}
                options={featureOptions}
                filterOption={(input, opt) => {
                  return String(opt?.label).toLowerCase().includes(input.toLowerCase());
                }}
              />
              <Select<ConditionOperator>
                value={cond.operator}
                onChange={(value) => updateCondition(idx, { operator: value })}
                options={CONDITION_OPERATOR_OPTIONS}
              />
              <Space.Compact style={{ width: '100%' }}>
                {cond.rhsIsFeature ? (
                  <Select
                    showSearch
                    placeholder="Variable"
                    value={cond.rhs || undefined}
                    onChange={(value) => updateCondition(idx, { rhs: value })}
                    options={featureOptions}
                    style={{ width: '100%' }}
                    filterOption={(input, opt) => {
                      return String(opt?.label).toLowerCase().includes(input.toLowerCase());
                    }}
                  />
                ) : (
                  <Input
                    placeholder="Value"
                    value={cond.rhs}
                    onChange={(e) => updateCondition(idx, { rhs: e.target.value })}
                  />
                )}
                <Button
                  onClick={() => updateCondition(idx, { rhs: '', rhsIsFeature: !cond.rhsIsFeature })}
                  title={cond.rhsIsFeature ? 'Use a literal value' : 'Use a defined variable'}
                >
                  {cond.rhsIsFeature ? 'var' : 'lit'}
                </Button>
              </Space.Compact>
              <Button
                type="text"
                icon={<DeleteOutlined />}
                onClick={() => removeCondition(idx)}
                disabled={model.conditions.length === 1}
              />
            </div>
          );
        })}
        <Button icon={<PlusOutlined />} onClick={addCondition}>
          Add condition
        </Button>
      </div>

      <div className={styles.builderSection}>
        <Title level={5}>Outcomes</Title>
        <Paragraph type="secondary" style={{ fontSize: 12 }}>
          What Osprey does when the conditions above are met, like adding a label or banning the user.
        </Paragraph>
        {model.outcomes.map((outcome, oIdx) => {
          return (
            <div key={oIdx} className={styles.builderSection}>
              <div className={styles.builderRowOutcome}>
                <Select
                  showSearch
                  placeholder="Effect"
                  value={outcome.effect || undefined}
                  onChange={(value) => {
                    return updateOutcome(oIdx, { effect: value, args: outcomeArgsForEffect(value, vocabulary.udfs) });
                  }}
                  options={effectOptions}
                  filterOption={(input, opt) => {
                    return String(opt?.label).toLowerCase().includes(input.toLowerCase());
                  }}
                />
                <Button
                  type="text"
                  icon={<DeleteOutlined />}
                  onClick={() => removeOutcome(oIdx)}
                  disabled={model.outcomes.length === 1}
                />
              </div>
              {outcome.args.length > 0 && (
                <div className={styles.builderArgsGrid}>
                  {outcome.args.map((arg, aIdx) => {
                    return (
                      // Keyed by index, not by name: a positional argument has no name,
                      // and two of them would collide on a null key.
                      <React.Fragment key={aIdx}>
                        <div className={styles.builderArgLabel}>
                          {arg.name ?? <Text type="secondary">#{aIdx + 1}</Text>}
                        </div>
                        <Space.Compact style={{ width: '100%' }}>
                          {arg.isFeature ? (
                            <Select
                              showSearch
                              placeholder="Variable"
                              value={arg.value || undefined}
                              onChange={(value) => updateOutcomeArg(oIdx, aIdx, { value })}
                              options={featureOptions}
                              style={{ width: '100%' }}
                              filterOption={(input, opt) => {
                                return String(opt?.label).toLowerCase().includes(input.toLowerCase());
                              }}
                            />
                          ) : (
                            <Input
                              placeholder="Value"
                              value={arg.value}
                              onChange={(e) => updateOutcomeArg(oIdx, aIdx, { value: e.target.value })}
                            />
                          )}
                          <Button
                            onClick={() => updateOutcomeArg(oIdx, aIdx, { value: '', isFeature: !arg.isFeature })}
                            title={arg.isFeature ? 'Use a literal value' : 'Use a defined variable'}
                          >
                            {arg.isFeature ? 'var' : 'lit'}
                          </Button>
                        </Space.Compact>
                      </React.Fragment>
                    );
                  })}
                </div>
              )}
            </div>
          );
        })}
        <Button icon={<PlusOutlined />} onClick={addOutcome}>
          Add outcome
        </Button>
      </div>
    </>
  );
};

function guessRuleNameFromSource(source: string): string {
  const m = source.match(/^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*Rule\s*\(/m);
  return m?.[1] ?? '';
}
