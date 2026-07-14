import * as React from 'react';
import {
  Alert,
  Button,
  Card,
  Checkbox,
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
import { useHistory, useLocation } from 'react-router-dom';

import {
  getRuleDraftSource,
  getRuleDraftVocabulary,
  parseRuleDraftIntoBuilder,
  submitRuleDraft,
  validateRuleDraft,
} from '../../actions/RulesActions';
import usePromiseResult from '../../hooks/usePromiseResult';
import {
  ParseIntoBuilderResponse,
  RuleDraftValidationMessage,
  RuleDraftValidationResponse,
  RuleDraftVocabulary,
} from '../../types/RulesTypes';
import { renderFromPromiseResult } from '../../utils/PromiseResultUtils';

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

type EditorMode = 'builder' | 'code';

const VALIDATE_DEBOUNCE_MS = 600;

interface BootstrapData {
  vocabulary: RuleDraftVocabulary;
  initialSource: string;
  initialPath: string;
  isNewRule: boolean;
  // For edit mode: the result of round-tripping the loaded source through the
  // backend parser. Determines whether the Rule Builder toggle is enabled and
  // what model the builder starts from.
  initialBuilderParse?: ParseIntoBuilderResponse;
}

export const RuleEditorPage: React.FC = () => {
  // The edit path lives in `?path=` because react-router v5 has no clean
  // repeating-segment param and rule paths contain slashes.
  const location = useLocation();
  const isNewRule = location.pathname === '/rules/new';
  const editPath = isNewRule ? undefined : (new URLSearchParams(location.search).get('path') ?? undefined);

  const result = usePromiseResult<BootstrapData>(async () => {
    const vocabulary = await getRuleDraftVocabulary();
    if (isNewRule) {
      return {
        vocabulary,
        initialSource: '',
        initialPath: 'rules/new_rule.sml',
        isNewRule: true,
      };
    }
    if (!editPath) {
      throw new Error('Missing ?path= query parameter; navigate from the Rules page.');
    }
    const source = await getRuleDraftSource(editPath);
    const initialBuilderParse = await parseRuleDraftIntoBuilder(source.path, source.contents);
    return {
      vocabulary,
      initialSource: source.contents,
      initialPath: source.path,
      isNewRule: false,
      initialBuilderParse,
    };
  }, [editPath, isNewRule]);

  return renderFromPromiseResult(result, (data) => {
    return <RuleEditorView data={data} />;
  });
};

const RuleEditorView: React.FC<{ data: BootstrapData }> = ({ data }) => {
  const history = useHistory();
  // Builder is allowed for new rules and for edits whose source round-trips.
  const builderAllowed = data.isNewRule || data.initialBuilderParse?.supported === true;
  const builderDisabledReason =
    !data.isNewRule && data.initialBuilderParse?.supported === false ? data.initialBuilderParse.reason : '';
  const [mode, setMode] = React.useState<EditorMode>(builderAllowed && data.isNewRule ? 'builder' : 'code');
  const [path, setPath] = React.useState<string>(data.initialPath);
  const [codeSource, setCodeSource] = React.useState<string>(data.initialSource);
  const [builder, setBuilder] = React.useState<RuleBuilderModel>(() => {
    if (data.initialBuilderParse?.supported === true) {
      return data.initialBuilderParse.model;
    }
    return EMPTY_BUILDER_MODEL;
  });
  const [summary, setSummary] = React.useState<string>('');
  // Off by default: turning a rule on is a deliberate opt-in, so a submit never
  // wires a new rule into the live ruleset unless the author checks the box.
  const [wireIntoMain, setWireIntoMain] = React.useState<boolean>(false);
  const [validation, setValidation] = React.useState<RuleDraftValidationResponse | null>(null);
  const [isValidating, setIsValidating] = React.useState<boolean>(false);
  const [submitState, setSubmitState] = React.useState<
    | { kind: 'idle' }
    | { kind: 'submitting' }
    | { kind: 'done'; title: string; prUrl: string | null }
    | { kind: 'error'; message: string }
  >({ kind: 'idle' });

  const effectiveSource = mode === 'builder' ? generateSmlFromBuilder(builder, data.vocabulary.features) : codeSource;

  React.useEffect(() => {
    let cancelled = false;
    if (!effectiveSource.trim()) {
      // eslint-disable-next-line react-hooks/set-state-in-effect -- clearing stale validation on input empty
      setValidation(null);
      setIsValidating(false);
      return;
    }
    setIsValidating(true);
    const handle = window.setTimeout(async () => {
      try {
        const result = await validateRuleDraft(path, effectiveSource);
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
    };
  }, [effectiveSource, path]);

  const ruleNameForSubmit = mode === 'builder' ? builder.ruleName : guessRuleNameFromSource(codeSource);

  const canSubmit =
    !!validation?.ok &&
    SML_IDENTIFIER_RE.test(ruleNameForSubmit) &&
    submitState.kind !== 'submitting' &&
    !!effectiveSource.trim();

  // The builder and code editor hold independent state, so a tab switch has to
  // carry content across: builder -> code dumps the generated SML into the
  // textarea, code -> builder re-parses the (possibly hand-edited) source so
  // the form never silently submits a stale model.
  const onModeChange = async (next: EditorMode) => {
    if (next === mode) return;
    if (next === 'code') {
      setCodeSource(generateSmlFromBuilder(builder, data.vocabulary.features));
      setMode('code');
      return;
    }
    if (!codeSource.trim()) {
      setMode('builder');
      return;
    }
    try {
      const parsed = await parseRuleDraftIntoBuilder(path, codeSource);
      if (parsed.supported) {
        setBuilder(parsed.model);
        setMode('builder');
      } else {
        message.warning(`Rule Builder can't represent this file: ${parsed.reason}. Keep editing in Code Editor.`);
      }
    } catch (e) {
      message.warning(`Could not parse this file for Rule Builder: ${e instanceof Error ? e.message : String(e)}`);
    }
  };

  const onSubmit = async () => {
    if (!canSubmit) return;
    setSubmitState({ kind: 'submitting' });
    try {
      const res = await submitRuleDraft({
        path,
        source: effectiveSource,
        rule_name: ruleNameForSubmit,
        summary,
        is_new_rule: data.isNewRule,
        wire_into_main: wireIntoMain,
      });
      setSubmitState({ kind: 'done', title: res.title, prUrl: res.url });
      const wiredMsg = res.main_sml_updated ? ' (main.sml updated)' : '';
      message.success(`${res.title}${wiredMsg}.`);
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      setSubmitState({ kind: 'error', message: msg });
    }
  };

  return (
    <div className={styles.viewContainer}>
      <div className={styles.scrollArea}>
        <div className={styles.headerRow}>
          <div className={styles.headerLeft}>
            <Title level={3} style={{ margin: 0 }}>
              {data.isNewRule ? 'Add rule' : 'Edit rule'}
            </Title>
            <Text type="secondary">
              Drafts open a pull request against the rules repo. Nothing applies until the PR is merged and the engine
              reloads.
            </Text>
          </div>
          <div className={styles.headerActions}>
            <Tooltip
              title={
                builderAllowed
                  ? ''
                  : `Rule Builder can't represent this file: ${builderDisabledReason}. Edit in Code Editor.`
              }
            >
              <Segmented
                value={mode}
                onChange={(value) => {
                  void onModeChange(value as EditorMode);
                }}
                options={[
                  { label: 'Rule Builder', value: 'builder', disabled: !builderAllowed },
                  { label: 'Code Editor', value: 'code' },
                ]}
              />
            </Tooltip>
            <Button onClick={() => history.push('/rules')}>Cancel</Button>
            <Button type="primary" icon={<SaveOutlined />} disabled={!canSubmit} onClick={onSubmit}>
              Submit for review
            </Button>
          </div>
        </div>

        <SubmitBanner submitState={submitState} />

        <div className={styles.editorGrid}>
          <div>
            <Card size="small" style={{ marginBottom: 12 }}>
              <Form layout="vertical" size="small">
                <Form.Item label="File path" tooltip="Path inside the rules repo where this file will live.">
                  <Input value={path} onChange={(e) => setPath(e.target.value)} disabled={!data.isNewRule} />
                </Form.Item>
                <Form.Item
                  label={data.isNewRule ? 'Why this rule? (for reviewers)' : "What's changing? (for reviewers)"}
                  tooltip="Becomes the pull request description. Not saved into the rule file itself."
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
                <Form.Item style={{ marginBottom: 0 }}>
                  <Checkbox checked={wireIntoMain} onChange={(e) => setWireIntoMain(e.target.checked)}>
                    Turn this rule on once the review is approved.
                  </Checkbox>
                  <div className={styles.footnote}>
                    Adds your rule to the list Osprey runs, as part of the same review. If it&apos;s already on the
                    list, nothing changes.
                  </div>
                </Form.Item>
              </Form>
            </Card>

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
              <RuleBuilderEditor model={builder} setModel={setBuilder} vocabulary={data.vocabulary} />
            ) : (
              <CodeEditorMode source={codeSource} setSource={setCodeSource} />
            )}

            {mode === 'builder' && (
              <Card size="small" title="Generated SML preview" style={{ marginTop: 12 }}>
                <pre className={styles.previewBlock}>{effectiveSource}</pre>
                <div className={styles.footnote}>
                  This is the code that will be submitted in a pull request on Github. Make further changes in the Code
                  Editor view.
                </div>
              </Card>
            )}
          </div>

          <aside className={styles.sidePanel}>
            <ValidationPanel validation={validation} isValidating={isValidating} />
            <VocabularyPanel vocabulary={data.vocabulary} />
          </aside>
        </div>
      </div>
    </div>
  );
};

const SubmitBanner: React.FC<{
  submitState:
    | { kind: 'idle' }
    | { kind: 'submitting' }
    | { kind: 'done'; title: string; prUrl: string | null }
    | { kind: 'error'; message: string };
}> = ({ submitState }) => {
  if (submitState.kind === 'idle') return null;
  if (submitState.kind === 'submitting') {
    return <Alert type="info" message="Submitting draft..." showIcon style={{ marginBottom: 12 }} />;
  }
  if (submitState.kind === 'done') {
    return (
      <Alert
        type="success"
        showIcon
        style={{ marginBottom: 12 }}
        message={submitState.title}
        description={
          submitState.prUrl ? (
            <a href={submitState.prUrl} target="_blank" rel="noopener noreferrer">
              {submitState.prUrl}
            </a>
          ) : null
        }
      />
    );
  }
  return (
    <Alert
      type="error"
      showIcon
      style={{ marginBottom: 12 }}
      message="Submit failed"
      description={submitState.message}
    />
  );
};

const CodeEditorMode: React.FC<{ source: string; setSource: (next: string) => void }> = ({ source, setSource }) => {
  return (
    <Card size="small" title="SML source">
      <Input.TextArea
        className={styles.codeArea}
        value={source}
        onChange={(e) => setSource(e.target.value)}
        placeholder="MyRule = Rule(when_all=[PostText == 'hello'], description='...')"
        autoSize={{ minRows: 24, maxRows: 60 }}
        spellCheck={false}
      />
    </Card>
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

const VocabularyPanel: React.FC<{ vocabulary: RuleDraftVocabulary }> = ({ vocabulary }) => {
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

const RuleBuilderEditor: React.FC<{
  model: RuleBuilderModel;
  setModel: React.Dispatch<React.SetStateAction<RuleBuilderModel>>;
  vocabulary: RuleDraftVocabulary;
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
    <Card size="small" title="Rule">
      <Form layout="vertical" size="small">
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
        <Title level={5} style={{ marginBottom: 8 }}>
          Conditions
        </Title>
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
        <Button size="small" icon={<PlusOutlined />} onClick={addCondition}>
          Add condition
        </Button>
      </div>

      <div className={styles.builderSection}>
        <Title level={5} style={{ marginBottom: 8 }}>
          Outcomes
        </Title>
        <Paragraph type="secondary" style={{ fontSize: 12 }}>
          Wrapped in a <code>WhenRules(then=[…])</code> block that fires when the rule matches.
        </Paragraph>
        {model.outcomes.map((outcome, oIdx) => {
          return (
            <div key={oIdx}>
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
                      <React.Fragment key={arg.name}>
                        <div className={styles.builderArgLabel}>{arg.name}</div>
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
        <Button size="small" icon={<PlusOutlined />} onClick={addOutcome}>
          Add outcome
        </Button>
      </div>
    </Card>
  );
};

function guessRuleNameFromSource(source: string): string {
  const m = source.match(/^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*Rule\s*\(/m);
  return m?.[1] ?? '';
}
