from typing import List, abc, abstractmethod
import nflapi.Client
from nflapidb.EntityManager import EntityManager
from nflapidb.QueryModel import QueryModel

class DataManagerFacade(abc.ABC):

    def __init__(self, entityName : str, entityManager : EntityManager, apiClient : nflapi.Client.Client = None):
        self._entity_name = entityName
        self._entity_manager = entityManager
        if apiClient is None:
            apiClient = nflapi.Client.Client()
        self._nflapi_client = apiClient

    @property
    def entityName(self) -> str:
        return self._entity_name

    @abstractmethod
    async def sync(self, **kwargs) -> List[dict]:
        pass

    async def save(self, data : List[dict]) -> List[dict]:
        return await self._entity_manager.save(self._entity_name, data)

    async def find(self, qm : QueryModel = None, **kwargs) -> List[dict]:
        if qm is None:
            qm = self._getQueryModel(**kwargs)
        return await self._entity_manager.find(self._entity_name,
                                               query=qm.constraint,
                                               projection=qm.select())

    async def delete(self, qm : QueryModel = None, **kwargs) -> int:
        if qm is None:
            qm = self._getQueryModel(**kwargs)
        return await self._entity_manager.delete(self._entity_name,
                                                 query=qm.constraint)

    async def drop(self):
        await self._entity_manager.drop(self._entity_name)

    @property
    def _entityManager(self) -> EntityManager:
        return self._entity_manager

    @property
    def _apiClient(self) -> nflapi.Client.Client:
        return self._nflapi_client
    
    @abstractmethod
    def _getQueryModel(self, **kwargs) -> QueryModel:
        """This is called by find and delete to get the query parameters"""
        pass