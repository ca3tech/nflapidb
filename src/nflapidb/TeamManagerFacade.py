from typing import List
import nflapi.Client
from nflapidb.EntityManager import EntityManager
from nflapidb.QueryModel import QueryModel, Operator

class TeamManagerFacade:

    def __init__(self, entityManager : EntityManager, apiClient : nflapi.Client.Client = None):
        self._entity_name = "team"
        self._entity_manager = entityManager
        if apiClient is None:
            apiClient = nflapi.Client.Client()
        self._nflapi_client = apiClient

    @property
    def entityName(self) -> str:
        return self._entity_name

    async def sync(self) -> List[dict]:
        return await self.save(self._findFromSource())

    async def save(self, data : List[dict]) -> List[dict]:
        return await self._entity_manager.save(self._entity_name, data)

    async def find(self, teams : List[str] = None) -> List[dict]:
        qm = QueryModel()
        if teams is not None:
            qm.cstart("team", teams, Operator.IN)
        return await self._entity_manager.find(self._entity_name,
                                               query=qm.constraint,
                                               projection=qm.select())

    async def drop(self):
        await self._entity_manager.drop(self._entity_name)
    
    def _findFromSource(self) -> List[dict]:
        teams = []
        for team in self._nflapi_client.getTeams():
            teams.append({"team": team})
        return teams