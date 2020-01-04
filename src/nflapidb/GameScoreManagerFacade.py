from typing import List
from nflapidb.ScheduleDependantManagerFacade import ScheduleDependantManagerFacade
import nflapi.Client
from nflapidb.EntityManager import EntityManager
from nflapidb.ScheduleManagerFacade import ScheduleManagerFacade
from nflapidb.QueryModel import QueryModel, Operator

class GameScoreManagerFacade(ScheduleDependantManagerFacade):

    def __init__(self, entityManager : EntityManager,
                 apiClient : nflapi.Client.Client = None,
                 scheduleManager : ScheduleManagerFacade = None):
        super(GameScoreManagerFacade, self).__init__("game_score", entityManager, apiClient, scheduleManager)

    async def find(self, qm : QueryModel = None,
                   gsis_ids : List[str] = None) -> List[dict]:
        return await super(GameScoreManagerFacade, self).find(qm=qm,
                                                              gsis_ids=gsis_ids)

    async def delete(self, qm : QueryModel = None, gsis_ids : List[str] = None) -> List[dict]:
        return await super(GameScoreManagerFacade, self).delete(qm=qm, gsis_ids=gsis_ids)

    def _queryAPI(self, schedules : List[dict]) -> List[dict]:
        return self._apiClient.getGameScore(schedules)

    def _getQueryModel(self, **kwargs) -> QueryModel:
        cmap = {
            "gsis_id": kwargs["gsis_ids"] if "gsis_ids" in kwargs else None
        }
        return super(GameScoreManagerFacade, self)._getQueryModel(cmap)
