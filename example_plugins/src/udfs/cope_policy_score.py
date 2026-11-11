"""CoPE-B-S1 policy-scoring UDF (Parasail free inference).

Scores a piece of text against a *written policy* and returns the probability
that it violates the policy, in [0, 1] (0 = clearly allowed, 1 = clearly
violating). Change the policy text in the rule to change what's enforced — no
retraining.

Uses the CoPE-B-S1 "System One" decision API (POST /v1/systemone): we send the
content as `state.content` plus one `noul` (true/false) question wrapping the
policy, and read `answers.q1.noul` = P(violation). No tokens are generated
(pooled prefill), so it's fast and cheap.

Runs on the sync gevent worker, so a blocking `requests` call is fine with
`execute_async = True` (it runs in a greenlet). Endpoint + key + model come from
env (Codespaces secrets): PARASAIL_API_KEY, optionally COPE_ENDPOINT_URL and
COPE_MODEL.
"""

import os
import time

import requests
from osprey.engine.executor.execution_context import ExecutionContext
from osprey.engine.stdlib.udfs.categories import UdfCategories
from osprey.engine.udf.arguments import ArgumentsBase, ConstExpr
from osprey.engine.udf.base import UDFBase

ENDPOINT = os.environ.get('COPE_ENDPOINT_URL', 'https://api.parasail.io').rstrip('/') + '/v1/systemone'
MODEL = os.environ.get('COPE_MODEL', 'parasail-cope-b-s1')
MAX_ATTEMPTS = 3


class CopePolicyScoreArguments(ArgumentsBase):
    text: str
    """The content to evaluate (e.g. a post's text)."""
    policy: ConstExpr[str]
    """The written policy, describing the content to flag (a literal in the rule)."""


class CopePolicyScore(UDFBase[CopePolicyScoreArguments, float]):
    """Returns P(the text violates the given policy), in [0, 1], via CoPE-B-S1 (System One)."""

    category = UdfCategories.HTTP
    execute_async = True

    def execute(self, execution_context: ExecutionContext, arguments: CopePolicyScoreArguments) -> float:
        api_key = os.environ.get('PARASAIL_API_KEY', '')
        body = {
            'model': MODEL,
            'questions': {
                'q1': {
                    'type': 'noul',
                    'instructions': 'Does the content meet the criteria described in the following policy?\n\n'
                    + arguments.policy.value,
                    'criteria': {
                        'true': "The content meets the policy's criteria.",
                        'false': "The content does not meet the policy's criteria.",
                    },
                }
            },
            'state': {'content': arguments.text},
        }
        headers = {'Authorization': f'Bearer {api_key}', 'Content-Type': 'application/json'}
        for attempt in range(1, MAX_ATTEMPTS + 1):
            try:
                resp = requests.post(ENDPOINT, json=body, headers=headers, timeout=30)
                if resp.status_code in (429, 529, 503) and attempt < MAX_ATTEMPTS:
                    time.sleep(0.25 * attempt)
                    continue
                resp.raise_for_status()
                return float(resp.json()['answers']['q1']['noul'])
            except (requests.RequestException, ValueError, KeyError):
                if attempt >= MAX_ATTEMPTS:
                    # Fail safe: treat as non-violating so a model/network blip never blocks the pipeline.
                    return 0.0
                time.sleep(0.25 * attempt)
        return 0.0
