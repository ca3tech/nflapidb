from typing import List
import logging
import nflapi.Client
from nflapidb.EntityManager import EntityManager
from nflapidb.PlayerSchedDepManagerFacade import PlayerSchedDepManagerFacade
from nflapidb.ScheduleManagerFacade import ScheduleManagerFacade
from nflapidb.RosterManagerFacade import RosterManagerFacade
from nflapidb.TeamManagerFacade import TeamManagerFacade
import nflapidb.Utilities as util
from nflapidb.QueryModel import QueryModel

class GamePlayManagerFacade(PlayerSchedDepManagerFacade):

    def __init__(self, entityManager : EntityManager,
                 apiClient : nflapi.Client.Client = None,
                 scheduleManager : ScheduleManagerFacade = None,
                 rosterManager : RosterManagerFacade = None,
                 teamManager : TeamManagerFacade = None):
        super(GamePlayManagerFacade, self).__init__("game_play", entityManager, apiClient, scheduleManager, rosterManager, teamManager)

    async def find(self, qm : QueryModel = None,
                   gsis_ids : List[str] = None,
                   player_ids : List[str] = None,
                   profile_ids : List[str] = None,
                   stat_ids : List[int] = None,
                   stat_cats : List[str] = None) -> List[dict]:
        return await super(GamePlayManagerFacade, self).find(qm=qm,
                                                             gsis_ids=gsis_ids,
                                                             player_ids=player_ids,
                                                             profile_ids=profile_ids,
                                                             stat_ids=stat_ids,
                                                             stat_cats=stat_cats)

    async def delete(self, gsis_ids : List[str] = None,
                     player_ids : List[str] = None,
                     profile_ids : List[str] = None) -> List[dict]:
        return await super(GamePlayManagerFacade, self).delete(gsis_ids=gsis_ids,
                                                               player_ids=player_ids,
                                                               profile_ids=profile_ids)

    def _queryAPI(self, schedules : List[dict]) -> List[dict]:
        logging.info("Retrieving {} data from NFL API...".format(self._entity_name))
        return self._apiClient.getGamePlay(schedules)

    def _getQueryModel(self, **kwargs) -> QueryModel:
        cmap = {
            "gsis_id": kwargs["gsis_ids"] if "gsis_ids" in kwargs else None,
            "player_id": kwargs["player_ids"] if "player_ids" in kwargs else None,
            "profile_id": kwargs["profile_ids"] if "profile_ids" in kwargs else None,
            "stat_id": kwargs["stat_ids"] if "stat_ids" in kwargs else None,
            "stat_cat": kwargs["stat_cats"] if "stat_cats" in kwargs else None
        }
        return super(GamePlayManagerFacade, self)._getQueryModel(cmap)
