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
        self._abbr_amb = {}
        self._abbr_miss = {}

    async def sync(self) -> List[dict]:
        logging.info("Syncing {} data...".format(self._entity_name))
        pidqm = QueryModel()
        pidqm.cstart("profile_id", False, Operator.EXISTS)
        udata = await self.find(qm=pidqm)
        if len(udata) > 0:
            await self._setProfileIds(udata)
            udata = [d for d in udata if "profile_id" in d]
        if len(udata) > 0:
            udata = await self.save(udata)
        data = await super(PlayerSchedDepManagerFacade, self).sync()
        if len(udata) > 0:
            if len(data) > 0:
                data = udata + data
            else:
                data = udata
        return data

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

    def _getPlayerAbbrevFailKey(self, gmrec : dict) -> str:
        pabbr = gmrec["player_abrv_name"]
        pteam = gmrec["team"]
        return "{}/{}".format(pabbr, pteam)

    def _getPlayerAbbrevFailValue(self, gmrec : dict) -> dict:
        return dict([(k, gmrec[k]) for k in ["player_abrv_name", "team"]])

    def _addMissingPlayerAbbrev(self, gmrec : dict):
        key = self._getPlayerAbbrevFailKey(gmrec)
        if not key in self._abbr_miss:
            self._abbr_miss[key] = self._getPlayerAbbrevFailValue(gmrec)

    def _getMissingPlayerAbbrevs(self) -> List[dict]:
        return list(self._abbr_miss.values())

    def _addAmbiguousPlayerAbbrev(self, gmrec : dict):
        key = self._getPlayerAbbrevFailKey(gmrec)
        if not key in self._abbr_amb:
            self._abbr_amb[key] = self._getPlayerAbbrevFailValue(gmrec)

    def _getAmbiguousPlayerAbbrevs(self) -> List[dict]:
        return list(self._abbr_amb.values())

    async def _setProfileIds(self, gsdata : List[dict]):
        logging.info("Adding profile ids to data...")
        rpcnt = len(gsdata) // 20
        rmgr = self._rosterManager
        i = 0
        for gsr in gsdata:
            if "profile_id" not in gsr and "player_abrv_name" in gsr and gsr["player_abrv_name"] is not None and gsr["player_abrv_name"] != "":
                rdata = await rmgr.find(teams=[gsr["team"]],
                                        player_abbreviations=[gsr["player_abrv_name"]])
                if len(rdata) == 0:
                    rdata = await rmgr.find(teams=[gsr["team"]],
                                            player_abbreviations=[gsr["player_abrv_name"]],
                                            include_previous_teams=True)
                if len(rdata) == 0:
                    ln = re.sub(r"^[^. ]+[. ]", "", gsr["player_abrv_name"])
                    rdata = await rmgr.find(teams=[gsr["team"]], player_abbreviations=[ln])
                if len(rdata) == 1:
                    gsr["profile_id"] = rdata[0]["profile_id"]
                elif len(rdata) == 0:
                    self._addMissingPlayerAbbrev(gsr)
                    # logging.info("Profile id retrieval failed; no records matching player abbreviation {} [{}]".format(gsr["player_abrv_name"], gsr["team"]))
                else:
                    self._addAmbiguousPlayerAbbrev(gsr)
                    # logging.info("Profile id retrieval failed; player abbreviation {} [{}] is ambiguous".format(gsr["player_abrv_name"], gsr["team"]))
            i += 1
            if i % rpcnt == 0:
                logging.info("{}% complete".format(5 * i // rpcnt))
        logging.info("Profile id addition complete")