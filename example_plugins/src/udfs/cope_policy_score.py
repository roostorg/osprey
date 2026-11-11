"""CoPE-B-S1 policy-scoring UDF (Zentropi free inference).

Scores a piece of text against a *written policy* and returns the probability
that it violates the policy, in [0, 1] (0 = clearly allowed, 1 = clearly
violating). Change the policy text in the rule to change what's enforced — no
retraining.

Runs on the sync gevent worker, so a blocking `requests` call is fine with
`execute_async = True` (it runs in a greenlet). The Zentropi API key is read
from the ZENTROPI_API_KEY env var (a Codespaces secret).
"""

import os

import requests
from osprey.engine.executor.execution_context import ExecutionContext
from osprey.engine.stdlib.udfs.categories import UdfCategories
from osprey.engine.udf.arguments import ArgumentsBase, ConstExpr
from osprey.engine.udf.base import UDFBase

ZENTROPI_URL = 'https://api.zentropi.ai/v1/label'


class CopePolicyScoreArguments(ArgumentsBase):
    text: str
    """The content to evaluate (e.g. a post's text)."""
    policy: ConstExpr[str]
    """The written policy, describing the content to flag (a literal in the rule)."""


class CopePolicyScore(UDFBase[CopePolicyScoreArguments, float]):
    """Returns P(the text violates the given policy), in [0, 1], via CoPE-B-S1."""

    category = UdfCategories.HTTP
    execute_async = True

    def execute(self, execution_context: ExecutionContext, arguments: CopePolicyScoreArguments) -> float:
        api_key = os.environ.get('ZENTROPI_API_KEY', '')
        try:
            resp = requests.post(
                ZENTROPI_URL,
                json={
                    'model': 'cope-b-s1',
                    'content_text': arguments.text,
                    'criteria_text': arguments.policy.value,
                },
                headers={'Authorization': f'Bearer {api_key}', 'Content-Type': 'application/json'},
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json()
            # Response: {"label": "0"|"1", "confidence": <float>}.
            # label "1" = violates -> confidence is P(violation); label "0" -> invert.
            label = str(data.get('label', '0'))
            confidence = float(data.get('confidence', 0.0))
            return confidence if label == '1' else 1.0 - confidence
        except (requests.RequestException, ValueError, KeyError):
            # Fail safe: treat as non-violating so a model/network blip never blocks the pipeline.
            return 0.0
