import functools
from typing import Iterable, Optional

import Levenshtein


def get_closest_string_within_threshold(
    string: str, candidate_strings: Iterable[str], distance_threshold: int = 3
) -> Optional[str]:
    """Given a string, return the string that is the closest (in terms of case-insensitive levenshtein edit distance)
    from the given candidate strings."""
    if len(string) <= distance_threshold or not candidate_strings:
        return None

    # We want to consider the string as lowercase for the purpose of distance scoring,
    # this lets us consider the distance of `FOO`, `foo` and `FoO` as 0.s
    distance_fn = functools.partial(Levenshtein.distance, string.lower())
    closest_string = min(candidate_strings, key=lambda s: distance_fn(s.lower()))
    closest_distance = distance_fn(closest_string.lower())
    if closest_distance <= distance_threshold:
        return closest_string
    else:
        return None
