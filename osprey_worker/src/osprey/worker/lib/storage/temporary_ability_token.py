import base64
import json
import os
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional

import pytz
from osprey.worker.lib.singletons import CONFIG
from osprey.worker.ui_api.osprey.lib.abilities import Ability
from sqlalchemy import BigInteger, Column, DateTime, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Session

from .postgres import Model, scoped_session, session_registries

DEFAULT_ABILITY_LIFETIME = timedelta(hours=8)
DEFAULT_TOKEN_CONSUMPTION_LIFETIME = timedelta(minutes=3)

# In-memory fallback store used only when DB is not initialized
_MEM_TOKENS: Dict[str, 'TemporaryAbilityToken'] = {}


enum_doc = """
Consumption results for TemporaryAbilityToken operations.
"""


class ConsumptionResult(Enum):
    SUCCESS = 0
    TOKEN_NOT_FOUND = 1
    TOKEN_ALREADY_USED = 2
    TOKEN_EXPIRED = 3


def _db_initialized(database: str = 'osprey_db') -> bool:
    return session_registries.get(database) is not None


class TemporaryAbilityToken(Model):
    """
    `temporary_ability_tokens` are tokens that can be consumed to grant a user temporary abilities.

    The token must be consumed before `must_be_consumed_before` and the abilities expire at `abilities_expire_at`
    """

    __tablename__ = 'temporary_ability_tokens'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    token = Column(Text, nullable=False)
    consumed_by_email = Column(Text, nullable=True)
    abilities_json = Column(JSONB, nullable=False)
    creation_origin = Column(Text, nullable=False)
    must_be_consumed_before = Column(DateTime, nullable=False)
    abilities_expire_at = Column(DateTime, nullable=False)
    created_at = Column(DateTime, nullable=False)

    @classmethod
    def create(cls, abilities: List[Ability[Any, Any]], creation_origin: str) -> 'TemporaryAbilityToken':
        """
        Adds a single `ability_token` to the database or in-memory store when DB unavailable.
        """
        now = datetime.now(tz=pytz.utc)
        token_value = base64.urlsafe_b64encode(os.urandom(33)).decode('utf-8')
        ability_token = TemporaryAbilityToken(
            token=token_value,
            abilities_json=[json.loads(ability.json()) for ability in abilities],
            creation_origin=creation_origin,
            must_be_consumed_before=now + DEFAULT_TOKEN_CONSUMPTION_LIFETIME,
            abilities_expire_at=now + DEFAULT_ABILITY_LIFETIME,
            created_at=now,
        )

        if _db_initialized():
            with scoped_session(commit=True) as session:
                session.add(ability_token)
        else:
            # Testing path without Postgres
            if CONFIG.instance().testing:
                _MEM_TOKENS[token_value] = ability_token
            else:
                # In non-testing environments, require DB
                raise Exception('Database osprey_db has not yet been initialized!')

        return ability_token

    @classmethod
    def _get_ability_token_for_update(cls, token: str, session: Session) -> Optional['TemporaryAbilityToken']:
        ability_token: Optional['TemporaryAbilityToken'] = (
            session.query(TemporaryAbilityToken)
            .with_for_update()  # This prevents double consumption by locking the row until the session is over
            .filter(TemporaryAbilityToken.token == token)
            .one_or_none()
        )
        return ability_token

    @classmethod
    def _test_only_pre_consume_lock(cls) -> None:
        pass

    @classmethod
    def consume(cls, token: str, user_email: str) -> ConsumptionResult:
        """
        Consumes a token and assigns the `abilities` to a `user_email`.
        Works with DB or in-memory fallback when testing without Postgres.
        """
        now = datetime.now(tz=pytz.utc)
        if _db_initialized():
            with scoped_session(commit=True) as session:
                ability_token = cls._get_ability_token_for_update(token, session)
                if not ability_token:
                    return ConsumptionResult.TOKEN_NOT_FOUND
                if ability_token.consumed_by_email is not None and ability_token.consumed_by_email != user_email:
                    if ability_token.consumed_by_email != user_email:
                        return ConsumptionResult.TOKEN_ALREADY_USED
                    else:
                        return ConsumptionResult.SUCCESS
                assert ability_token.must_be_consumed_before is not None
                if ability_token.must_be_consumed_before < now:
                    return ConsumptionResult.TOKEN_EXPIRED

                ability_token.consumed_by_email = user_email
                cls._test_only_pre_consume_lock()  # needed to simulate multiple simultaneous consumptions when testing
                session.add(ability_token)
                return ConsumptionResult.SUCCESS
        else:
            if not CONFIG.instance().testing:
                raise Exception('Database osprey_db has not yet been initialized!')
            ability_token = _MEM_TOKENS.get(token)
            if not ability_token:
                return ConsumptionResult.TOKEN_NOT_FOUND
            if ability_token.consumed_by_email is not None and ability_token.consumed_by_email != user_email:
                return ConsumptionResult.TOKEN_ALREADY_USED
            assert ability_token.must_be_consumed_before is not None
            if ability_token.must_be_consumed_before < now:
                return ConsumptionResult.TOKEN_EXPIRED
            ability_token.consumed_by_email = user_email
            _MEM_TOKENS[token] = ability_token
            return ConsumptionResult.SUCCESS

    @classmethod
    def get_abilities(cls, email: str) -> List[Ability[Any, Any]]:
        """
        Gets all the temporary abilities associated with `email` that haven't expired yet
        """
        now = datetime.now(tz=pytz.utc)
        if _db_initialized():
            with scoped_session() as session:
                access_tokens: List['TemporaryAbilityToken'] = (
                    session.query(TemporaryAbilityToken)  # type: ignore # query is untyped
                    .filter(TemporaryAbilityToken.consumed_by_email == email)
                    .filter(TemporaryAbilityToken.abilities_expire_at > now)
                )
                return [ability for ability_token in access_tokens for ability in ability_token.abilities()]
        else:
            if not CONFIG.instance().testing:
                raise Exception('Database osprey_db has not yet been initialized!')
            return [
                ability
                for ability_token in _MEM_TOKENS.values()
                if ability_token.consumed_by_email == email
                and ability_token.abilities_expire_at is not None
                and ability_token.abilities_expire_at > now
                for ability in ability_token.abilities()
            ]

    def abilities(self) -> List[Ability[Any, Any]]:
        return [Ability.validate(ability_json) for ability_json in self.abilities_json]  # type: ignore # mypy doesn't like the list comprehension
