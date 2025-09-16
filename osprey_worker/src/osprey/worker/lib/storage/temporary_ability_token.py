import base64
import json
import os
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, List, Optional

import pytz
from osprey.worker.ui_api.osprey.lib.abilities import Ability
from sqlalchemy import BigInteger, Column, DateTime, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Session

from .postgres import Model, scoped_session

DEFAULT_ABILITY_LIFETIME = timedelta(hours=8)
DEFAULT_TOKEN_CONSUMPTION_LIFETIME = timedelta(minutes=3)


class ConsumptionResult(Enum):
    SUCCESS = 0
    TOKEN_NOT_FOUND = 1
    TOKEN_ALREADY_USED = 2
    TOKEN_EXPIRED = 3


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
        Adds a single `ability_token` to the database
        """
        with scoped_session(commit=True) as session:
            now = datetime.now(tz=pytz.utc)
            ability_token = TemporaryAbilityToken(
                token=base64.urlsafe_b64encode(os.urandom(33)).decode('utf-8'),
                # Dumping then loading the json is needed to get the ability in a form that postgres can accept
                abilities_json=[json.loads(ability.json()) for ability in abilities],
                creation_origin=creation_origin,
                must_be_consumed_before=now + DEFAULT_TOKEN_CONSUMPTION_LIFETIME,
                abilities_expire_at=now + DEFAULT_ABILITY_LIFETIME,
                created_at=now,
            )
            session.add(ability_token)

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
        Consumes a token and assigns the `abilities` to a `user_email`
        """
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
            if ability_token.must_be_consumed_before < datetime.now(tz=pytz.utc):
                return ConsumptionResult.TOKEN_EXPIRED

            ability_token.consumed_by_email = user_email
            cls._test_only_pre_consume_lock()  # needed to simulate multiple simultaneous consumptions when testing
            session.add(ability_token)
            return ConsumptionResult.SUCCESS

    @classmethod
    def get_abilities(cls, email: str) -> List[Ability[Any, Any]]:
        """
        Gets all the temporary abilities associated with `email` that haven't expired yet
        """
        with scoped_session() as session:
            access_tokens: List['TemporaryAbilityToken'] = (
                session.query(TemporaryAbilityToken)  # type: ignore # query is untyped
                .filter(TemporaryAbilityToken.consumed_by_email == email)
                .filter(TemporaryAbilityToken.abilities_expire_at > datetime.now(tz=pytz.utc))
            )
            return [ability for ability_token in access_tokens for ability in ability_token.abilities()]

    def abilities(self) -> List[Ability[Any, Any]]:
        return [Ability.validate(ability_json) for ability_json in self.abilities_json]  # type: ignore # mypy doesn't like the list comprehension
