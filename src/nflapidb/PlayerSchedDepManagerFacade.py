from typing import List
import logging
import re
import nflapi.Client
from nflapidb.EntityManager import EntityManager
from nflapidb.ScheduleDependantManagerFacade import ScheduleDependantManagerFacade
from nflapidb.ScheduleManagerFacade import ScheduleManagerFacade
from nflapidb.RosterManagerFacade import RosterManagerFacade
from nflapidb.TeamManagerFacade import TeamManagerFacade
from nflapidb.QueryModel import QueryModel, Operator

class PlayerSchedDepManagerFacade(ScheduleDependantManagerFacade):

    def __init__(self, entityName : str, entityManager : EntityManager,
                 apiClient : nflapi.Client.Client = None,
                 scheduleManager : ScheduleManagerFacade = None,
                 rosterManager : RosterManagerFacade = None,
                 teamManager : TeamManagerFacade = None):
        super(PlayerSchedDepManagerFacade, self).__init__(entityName, entityManager, apiClient, scheduleManager)
        self._rostmgr = rosterManager
        self._tmgr = teamManager

    async def save(self, data : List[dict]) -> List[dict]:
        logging.info("Saving {} data...".format(self._entity_name))
        if len(data) > 0:
            await self._setProfileIds(data)
            data = await super(PlayerSchedDepManagerFacade, self).save(data)
        return data

    @property
    def _rosterManager(self):
        if self._rostmgr is None:
            self._rostmgr = RosterManagerFacade(self._entityManager, self._apiClient, self._tmgr)
        return self._rostmgr

    async def _setProfileIds(self, gsdata : List[dict]):
        logging.info("Adding profile ids to data...")
        rmgr = self._rosterManager
        for gsr in gsdata:
            if "player_abrv_name" in gsr and gsr["player_abrv_name"] is not None and gsr["player_abrv_name"] != "":
                rdata = await rmgr.find(teams=[gsr["team"]],
                                        player_abbreviations=[gsr["player_abrv_name"]])
                if len(rdata) == 0:
                    ln = re.sub(r"^[^. ]+[. ]", "", gsr["player_abrv_name"])
                    rdata = await rmgr.find(teams=[gsr["team"]], player_abbreviations=[ln])
                if len(rdata) == 1:
                    gsr["profile_id"] = rdata[0]["profile_id"]
                elif len(rdata) == 0:
                    logging.info("Profile id retrieval failed; no records matching player abbreviation {}".format(gsr["player_abrv_name"]))
                else:
                    logging.info("Profile id retrieval failed; player abbreviation {} is ambiguous".format(gsr["player_abrv_name"]))
        logging.info("Profile id addition complete")