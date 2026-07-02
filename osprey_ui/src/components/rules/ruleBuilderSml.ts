import {
  ConditionOperator,
  RuleBuilderCondition,
  RuleBuilderModel,
  RuleBuilderOutcome,
  RuleBuilderOutcomeArg,
  RuleDraftVocabularyFeature,
  RuleDraftVocabularyUdf,
} from '../../types/RulesTypes';

// Re-export shared types so existing imports from this module keep working.
export type {
  ConditionOperator,
  RuleBuilderCondition as Condition,
  RuleBuilderModel,
  RuleBuilderOutcome as Outcome,
  RuleBuilderOutcomeArg as OutcomeArg,
} from '../../types/RulesTypes';

export const CONDITION_OPERATOR_OPTIONS: { value: ConditionOperator; label: string }[] = [
  { value: '==', label: 'is equal to' },
  { value: '!=', label: 'is not equal to' },
  { value: '>', label: 'is greater than' },
  { value: '<', label: 'is less than' },
  { value: '>=', label: 'is greater than or equal to' },
  { value: '<=', label: 'is less than or equal to' },
  { value: 'includes', label: 'includes' },
  { value: 'excludes', label: 'excludes' },
];

export const EMPTY_BUILDER_MODEL: RuleBuilderModel = {
  ruleName: '',
  description: '',
  conditions: [{ feature: '', operator: '==', rhs: '', rhsIsFeature: false }],
  outcomes: [{ effect: '', args: [] }],
};

export const SML_IDENTIFIER_RE = /^[A-Za-z_][A-Za-z0-9_]*$/;

// SML string literals follow Python escaping rules (the parser is Python's ast
// module), so backslashes must be escaped before quotes or a trailing `\` in
// user input would swallow the closing quote and inject raw SML.
function smlString(value: string): string {
  const escaped = value.replace(/\\/g, '\\\\').replace(/'/g, "\\'").replace(/\r/g, '\\r').replace(/\n/g, '\\n');
  return `'${escaped}'`;
}

function renderRhs(rhs: string, isFeature: boolean): string {
  if (isFeature) return rhs;
  if (rhs === '') return "''";
  if (/^-?\d+(\.\d+)?$/.test(rhs)) return rhs;
  if (rhs === 'true' || rhs === 'false') return rhs;
  return smlString(rhs);
}

function renderConditionExpression(c: RuleBuilderCondition): string {
  const lhs = c.feature || '__missing_feature__';
  const rhs = renderRhs(c.rhs, c.rhsIsFeature);
  switch (c.operator) {
    case 'includes':
      return `TextContains(text=${lhs}, phrase=${rhs})`;
    case 'excludes':
      return `not TextContains(text=${lhs}, phrase=${rhs})`;
    default:
      return `${lhs} ${c.operator} ${rhs}`;
  }
}

function renderOutcomeCall(o: RuleBuilderOutcome): string {
  const effect = o.effect || '__missing_effect__';
  // Skip args left blank: emitting `entity=` is a syntax error, and emitting `expires_after=''`
  // forces an empty literal where the UDF would otherwise use its default.
  const parts = o.args
    .filter((arg) => {
      return arg.value !== '';
    })
    .map((arg) => {
      return `${arg.name}=${renderRhs(arg.value, arg.isFeature)}`;
    });
  return `${effect}(${parts.join(', ')})`;
}

function collectReferencedFeatures(model: RuleBuilderModel): Set<string> {
  const refs = new Set<string>();
  for (const c of model.conditions) {
    if (c.feature) refs.add(c.feature);
    if (c.rhsIsFeature && c.rhs) refs.add(c.rhs);
  }
  for (const o of model.outcomes) {
    for (const arg of o.args) {
      if (arg.isFeature && arg.value) refs.add(arg.value);
    }
  }
  return refs;
}

function buildImportBlock(refs: Set<string>, features: RuleDraftVocabularyFeature[]): string {
  // Map each referenced identifier to the SML file it's defined in, drop main.sml
  // (the entry point cannot import itself), dedupe, and emit a single Import block.
  const paths = new Set<string>();
  for (const ref of refs) {
    const feat = features.find((f) => {
      return f.name === ref;
    });
    if (!feat) continue;
    if (feat.source_path === 'main.sml') continue;
    paths.add(feat.source_path);
  }
  if (paths.size === 0) return '';
  const sorted = [...paths].sort();
  const entries = sorted
    .map((p) => {
      return `    '${p}',`;
    })
    .join('\n');
  return `Import(
  rules=[
${entries}
  ]
)

`;
}

export function generateSmlFromBuilder(model: RuleBuilderModel, features: RuleDraftVocabularyFeature[] = []): string {
  // Never interpolate a non-identifier rule name: `Foo = Rule(...) # ` as a
  // "name" would otherwise inject arbitrary SML that still validates.
  const ruleName = SML_IDENTIFIER_RE.test(model.ruleName) ? model.ruleName : 'UnnamedRule';
  const whenAll = model.conditions
    .map((c) => {
      return `    ${renderConditionExpression(c)},`;
    })
    .join('\n');
  const ruleBlock = `${ruleName} = Rule(
  when_all=[
${whenAll}
  ],
  description=${smlString(model.description)},
)`;

  const imports = buildImportBlock(collectReferencedFeatures(model), features);

  if (model.outcomes.length === 0) {
    return `${imports}${ruleBlock}\n`;
  }

  const thenBlock = model.outcomes
    .map((o) => {
      return `    ${renderOutcomeCall(o)},`;
    })
    .join('\n');

  return `${imports}${ruleBlock}

WhenRules(
  rules_any=[${ruleName}],
  then=[
${thenBlock}
  ],
)
`;
}

/**
 * Insert or merge the given source paths into the file's top-level
 * `Import(rules=[...])` block. If an Import block already exists, missing
 * entries are added in-place; otherwise a new block is prepended.
 */
export function applyMissingImports(source: string, pathsToAdd: string[]): string {
  if (pathsToAdd.length === 0) return source;
  const existingMatch = source.match(/Import\s*\(\s*rules\s*=\s*\[([^\]]*)\]\s*\)/m);
  if (existingMatch) {
    const existingPathsBlock = existingMatch[1];
    const existing = new Set(
      Array.from(existingPathsBlock.matchAll(/['"]([^'"]+)['"]/g)).map((m) => {
        return m[1];
      })
    );
    const merged = new Set(existing);
    for (const p of pathsToAdd) merged.add(p);
    if (merged.size === existing.size) return source;
    const sorted = [...merged].sort();
    const entries = sorted
      .map((p) => {
        return `    '${p}',`;
      })
      .join('\n');
    const replacement = `Import(\n  rules=[\n${entries}\n  ]\n)`;
    return source.replace(existingMatch[0], replacement);
  }
  const sorted = [...pathsToAdd].sort();
  const entries = sorted
    .map((p) => {
      return `    '${p}',`;
    })
    .join('\n');
  return `Import(\n  rules=[\n${entries}\n  ]\n)\n\n${source}`;
}

export function outcomeArgsForEffect(effectName: string, udfs: RuleDraftVocabularyUdf[]): RuleBuilderOutcomeArg[] {
  const match = udfs.find((u) => {
    return u.name === effectName;
  });
  if (!match) return [];
  return match.arguments.map((arg) => {
    // Defaults the value-vs-feature toggle for the form. Wrong guesses are one click to flip.
    const looksLikeFeature = arg.name === 'entity' || /Id$|DID$/i.test(arg.name);
    return { name: arg.name, value: '', isFeature: looksLikeFeature };
  });
}
