import globToRegExp from 'glob-to-regexp';

export default class DefaultFeature {
  features: readonly string[];

  private actionNameSet: Set<string>;
  private actionNameRegularExpressions: RegExp[];

  constructor(actionNames: string[], features: string[]) {
    this.features = features;

    this.actionNameRegularExpressions = [];
    this.actionNameSet = new Set();

    for (const actionName of actionNames) {
      if (actionName.includes('*') || actionName.includes('?')) {
        this.actionNameRegularExpressions.push(globToRegExp(actionName));
      } else {
        this.actionNameSet.add(actionName);
      }
    }
  }

  appliesTo(actionName: string): boolean {
    return this.actionNameSet.has(actionName) || this.actionNameRegularExpressions.some((re) => re.test(actionName));
  }
}
