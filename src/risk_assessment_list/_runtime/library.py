from __future__ import annotations

from typing import Iterable

from ..models import (
    MixtureComponent,
    MixtureResult,
    SubstanceCandidate,
    SubstanceResult,
)
from .assessment import evaluate_mixture as evaluate_mixture_with_store
from .assessment import evaluate_substance as evaluate_substance_with_store
from .search import search_substances as search_substances_with_store
from .store import RuntimeStore


class RiskAssessmentList:
    def __init__(self, db_path: str | None = None) -> None:
        self._store = RuntimeStore(db_path)

    @property
    def connection(self):
        return self._store.connection

    def search_substances(
        self,
        query: str,
        limit: int = 10,
        mode: str = "balanced",
    ) -> list[SubstanceCandidate]:
        return search_substances_with_store(
            self._store,
            query=query,
            limit=limit,
            mode=mode,
        )

    def evaluate_substance(self, identifier: str) -> SubstanceResult:
        return evaluate_substance_with_store(self._store, identifier)

    def evaluate_mixture(self, components: Iterable[MixtureComponent]) -> MixtureResult:
        return evaluate_mixture_with_store(self._store, components)


_default = RiskAssessmentList()


def search_substances(
    query: str,
    limit: int = 10,
    mode: str = "balanced",
) -> list[SubstanceCandidate]:
    return _default.search_substances(query=query, limit=limit, mode=mode)


def evaluate_substance(identifier: str) -> SubstanceResult:
    return _default.evaluate_substance(identifier=identifier)


def evaluate_mixture(components: Iterable[MixtureComponent]) -> MixtureResult:
    return _default.evaluate_mixture(components=components)
