from typing import List
import nflapi.Client
from nflapidb.EntityManager import EntityManager
from nflapidb.ScheduleDependantManagerFacade import ScheduleDependantManagerFacade
from nflapidb.ScheduleManagerFacade import ScheduleManagerFacade
from nflapidb.RosterManagerFacade import RosterManagerFacade
from nflapidb.TeamManagerFacade import TeamManagerFacade
from nflapidb.QueryModel import QueryModel, Operator
import nflapidb.Utilities as util

class GameSummaryManagerFacade(ScheduleDependantManagerFacade):

    def __init__(self, entityManager : EntityManager,
                 apiClient : nflapi.Client.Client = None,
                 scheduleManager : ScheduleManagerFacade = None,
                 rosterManager : RosterManagerFacade = None,
                 teamManager : TeamManagerFacade = None):
        super(GameSummaryManagerFacade, self).__init__("game_summary", entityManager, apiClient, scheduleManager)
        self._rostmgr = rosterManager
        self._tmgr = teamManager

    async def save(self, data : List[dict]) -> List[dict]:
        if len(data) > 0:
            await self._setProfileIds(data)
            data = await super(GameSummaryManagerFacade, self).save(data)
        return data

    async def find(self, qm : QueryModel = None,
                   gsis_ids : List[str] = None,
                   player_ids : List[str] = None,
                   profile_ids : List[str] = None) -> List[dict]:
        return await super(GameSummaryManagerFacade, self).find(qm=qm,
                                                                gsis_ids=gsis_ids,
                                                                player_ids=player_ids,
                                                                profile_ids=profile_ids)

    async def delete(self, gsis_ids : List[str] = None,
                     player_ids : List[str] = None,
                     profile_ids : List[str] = None) -> List[dict]:
        return await super(GameSummaryManagerFacade, self).delete(gsis_ids=gsis_ids,
                                                                  player_ids=player_ids,
                                                                  profile_ids=profile_ids)

    def _queryAPI(self, schedules : List[dict]) -> List[dict]:
        return self._apiClient.getGameSummary(schedules)

    @property
    def _rosterManager(self):
        if self._rostmgr is None:
            self._rostmgr = RosterManagerFacade(self._entityManager, self._apiClient, self._tmgr)
        return self._rostmgr

    def _getQueryModel(self, **kwargs) -> QueryModel:
        qm = QueryModel()
        cmap = {
            "gsis_id": kwargs["gsis_ids"] if "gsis_ids" in kwargs else None,
            "player_id": kwargs["player_ids"] if "player_ids" in kwargs else None,
            "profile_id": kwargs["profile_ids"] if "profile_ids" in kwargs else None
        }
        for name in cmap:
            if cmap[name] is not None:
                qm.cand(name, cmap[name], Operator.IN)
        return qm

    async def _setProfileIds(self, gsdata : List[dict]):
        rmgr = self._rosterManager
        for gsr in gsdata:
            if "player_abrv_name" in gsr:
                rdata = await rmgr.find(teams=[gsr["team"]],
                                        player_abbreviations=[gsr["player_abrv_name"]])
                if len(rdata) == 1:
                    gsr["profile_id"] = rdata[0]["profile_id"]