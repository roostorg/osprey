export function pluralize(word: string, val: number, pluralization: string = 's'): string {
  if (val === 1) return word;

  return `${word}${pluralization}`;
}

/**
 * Splits a string into an array of objects containing substrings and their match status.
 * Supports both exact string matching and regex patterns. Matched substrings are included in the result.
 * Useful for tokenizing a string so that substrings matching a search query can be highlighted.
 *
 * @param str - The string to split
 * @param pattern - The substring or regex pattern to match against
 * @param useRegex - Whether to treat the pattern as a regex (default: false)
 * @returns Array of objects with substring and matched properties
 * @throws {Error} When useRegex is true and pattern is an invalid regex
 *
 * @example
 * ```ts
 * // Exact string matching
 * const result1 = splitStringIncludingMatches("foo boor", "oo");
 * // returns [
 * //   { substring: "f", matched: false },
 * //   { substring: "oo", matched: true },
 * //   { substring: " b", matched: false },
 * //   { substring: "oo", matched: true },
 * //   { substring: "r", matched: false }
 * // ]
 *
 * // Regex matching
 * const result2 = splitStringIncludingMatches("foo123bar456", "\\d+", true);
 * // returns [
 * //   { substring: "foo", matched: false },
 * //   { substring: "123", matched: true },
 * //   { substring: "bar", matched: false },
 * //   { substring: "456", matched: true }
 * // ]
 * ```
 */
export function splitStringIncludingMatches(
  str: string,
  pattern: string,
  useRegex: boolean = false
): Array<{ substring: string; matched: boolean }> {
  // Early returns for edge cases
  if (!str) return [];
  if (!pattern) return [{ substring: str, matched: false }];

  const result = useRegex ? splitStringRegexMatch(str, pattern) : splitStringExactMatch(str, pattern);

  // Filter out empty substrings (except for edge cases where they might be meaningful)
  return result.filter((item) => item.substring.length > 0);
}

export function highlightMatchedText(text: Array<{ substring: string; matched: boolean }>, highlightClass?: string) {
  const highlightText = (content: string) => {
    return (
      <span className={highlightClass ?? ''} style={{ backgroundColor: highlightClass ? 'inherit' : 'yellow' }}>
        {content}
      </span>
    );
  };
  const highlighted = text.map((item) => (item.matched ? highlightText(item.substring) : item.substring));
  return <>{highlighted}</>;
}

/**
 * Helper function to split string using exact text matching
 */
function splitStringExactMatch(str: string, pattern: string): Array<{ substring: string; matched: boolean }> {
  const result: Array<{ substring: string; matched: boolean }> = [];
  let remaining = str;
  let index = remaining.indexOf(pattern);

  while (index !== -1) {
    // Add non-matching substring before the match
    const beforeMatch = remaining.substring(0, index);
    if (beforeMatch) {
      result.push({ substring: beforeMatch, matched: false });
    }

    // Add the matched substring
    result.push({ substring: pattern, matched: true });

    remaining = remaining.substring(index + pattern.length);
    index = remaining.indexOf(pattern);
  }

  // Add any remaining non-matching substring
  if (remaining) {
    result.push({ substring: remaining, matched: false });
  }

  return result;
}

/**
 * Helper function to split string using regex pattern matching
 */
function splitStringRegexMatch(str: string, pattern: string): Array<{ substring: string; matched: boolean }> {
  const result: Array<{ substring: string; matched: boolean }> = [];
  let regex: RegExp;

  try {
    regex = new RegExp(pattern, 'g');
  } catch (error) {
    // invalid regex can't match in the string, so no need to force error handling upstream.
    return [{ substring: str, matched: false }];
  }

  let lastIndex = 0;
  let match: RegExpExecArray | null;

  while ((match = regex.exec(str)) !== null) {
    // Add non-matching substring before the match
    if (match.index > lastIndex) {
      const nonMatchSubstring = str.substring(lastIndex, match.index);
      if (nonMatchSubstring) {
        result.push({ substring: nonMatchSubstring, matched: false });
      }
    }

    // Add the matched substring
    result.push({ substring: match[0], matched: true });
    lastIndex = regex.lastIndex;

    // Prevent infinite loop for zero-length matches
    if (match[0].length === 0) {
      regex.lastIndex++;
    }
  }

  // Add any remaining non-matching substring
  if (lastIndex < str.length) {
    result.push({ substring: str.substring(lastIndex), matched: false });
  }

  return result;
}
