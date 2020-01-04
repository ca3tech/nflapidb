from typing import List
from nflapidb.ScheduleDependantManagerFacade import ScheduleDependantManagerFacade
import nflapi.Client
from nflapidb.EntityManager import EntityManager
from nflapidb.ScheduleManagerFacade import ScheduleManagerFacade
from nflapidb.QueryModel import QueryModel, Operator

class GameDriveManagerFacade(ScheduleDependantManagerFacade):

    def __init__(self, entityManager : EntityManager,
                 apiClient : nflapi.Client.Client = None,
                 scheduleManager : ScheduleManagerFacade = None):
        super(GameDriveManagerFacade, self).__init__("game_drive", entityManager, apiClient, scheduleManager)

    async def find(self, qm : QueryModel = None,
                   gsis_ids : List[str] = None,
                   drive_ids : List[str] = None) -> List[dict]:
        return await super(GameDriveManagerFacade, self).find(qm=qm,
                                                              gsis_ids=gsis_ids,
                                                              drive_ids=drive_ids)

    async def delete(self, qm : QueryModel = None, gsis_ids : List[str] = None) -> List[dict]:
        return await super(GameDriveManagerFacade, self).delete(qm=qm, gsis_ids=gsis_ids)

    def _queryAPI(self, schedules : List[dict]) -> List[dict]:
        return self._apiClient.getGameDrive(schedules)

    def _getQueryModel(self, **kwargs) -> QueryModel:
        cmap = {
            "gsis_id": kwargs["gsis_ids"] if "gsis_ids" in kwargs else None,
            "drive_id": kwargs["drive_ids"] if "drive_ids" in kwargs else None
        }
        return super(GameDriveManagerFacade, self)._getQueryModel(cmap)
