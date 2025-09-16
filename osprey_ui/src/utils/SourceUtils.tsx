const GITHUB_REPO_URL = 'https://github.com/roostorg/osprey/tree/main/example_rules';

export function getSourcesUrl({ path, lineNumber }: PathInfo): string {
  return `${GITHUB_REPO_URL}${path}#L${lineNumber}`;
}

export interface PathInfo {
  path: string;
  lineNumber: number;
  rest: string;
}

export function parsePathString(path: string): PathInfo | null {
  const match = /^(.*?):(\d+)(.*)$/.exec(path);
  if (match == null) {
    return null;
  }

  return {
    path: match[1],
    lineNumber: +match[2],
    rest: match[3],
  };
}
