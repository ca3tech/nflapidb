from typing import List
import re
import os
import json
import logging
import nflapi.Client
from nflapidb.EntityManager import EntityManager
from nflapidb.DataManagerFacade import DataManagerFacade
from nflapidb.QueryModel import QueryModel, Operator
from nflapidb.TeamManagerFacade import TeamManagerFacade
import nflapidb.Utilities as util

class RosterManagerFacade(DataManagerFacade):

    def __init__(self, entityManager : EntityManager,
                 apiClient : nflapi.Client.Client = None,
                 teamManager : TeamManagerFacade = None):
        super(RosterManagerFacade, self).__init__("roster", entityManager, apiClient)
        self._tmgr = teamManager

    async def sync(self) -> List[dict]:
        logging.info("Syncing roster data...")
        tmgr = self._teamManager
        trecs = await tmgr.find()
        teams = [rec["team"] for rec in trecs]
        data = []
        logging.info("Retrieving current roster data...")
        crosters = await self.find()
        if len(crosters) == 0:
            # We only initialize the collection with historic roster
            # data if there is no data currently loaded
            logging.info("Retrieving historic roster data...")
            hrdata = self._getHistoricData()
            if hrdata is not None and len(hrdata) > 0:
                logging.info("Saving historic roster data...")
                data.extend(await self.save(hrdata))
        logging.info("Retrieving rosters from NFL API...")
        data.extend(await self.save(self._apiClient.getRoster(teams)))
        return data

    async def save(self, data : List[dict]) -> List[dict]:
        logging.info("Saving {} rosters...".format(len(data)))
        if len(data) > 0:
            cdata = await self._setPreviousTeams(data)
            data = await super(RosterManagerFacade, self).save(cdata)
        return data

    async def find(self, teams : List[str] = None,
                   positions : List[str] = None,
                   last_names : List[str] = None,
                   first_names : List[str] = None,
                   profile_ids : List[int] = None,
                   player_abbreviations : List[str] = None,
                   include_previous_teams : bool = False) -> List[dict]:
        return await super(RosterManagerFacade, self).find(teams=teams, positions=positions,
                                                           last_names=last_names,
                                                           first_names=first_names,
                                                           profile_ids=profile_ids,
                                                           player_abbreviations=player_abbreviations,
                                                           include_previous_teams=include_previous_teams)

    async def delete(self, teams : List[str] = None,
                     profile_ids : List[int] = None) -> List[dict]:
        return await super(RosterManagerFacade, self).delete(teams=teams,
                                                             profile_ids=profile_ids)

    @property
    def _teamManager(self) -> TeamManagerFacade:
        if self._tmgr is None:
            self._tmgr = TeamManagerFacade(self._entityManager, self._apiClient)
        return self._tmgr

    def _getQueryModel(self, **kwargs) -> QueryModel:
        qm = QueryModel()
        if "teams" in kwargs and kwargs["teams"] is not None:
            qm.cstart("team", kwargs["teams"], Operator.IN)
            if "include_previous_teams" in kwargs and kwargs["include_previous_teams"]:
                qm.cor("previous_teams", kwargs["teams"], Operator.IN)
        cmap = {
            "position": kwargs["positions"] if "positions" in kwargs else None,
            "last_name": kwargs["last_names"] if "last_names" in kwargs else None,
            "first_name": kwargs["first_names"] if "first_names" in kwargs else None,
            "profile_id": kwargs["profile_ids"] if "profile_ids" in kwargs else None
        }
        for name in cmap:
            if cmap[name] is not None:
                qm.cand(name, cmap[name], Operator.IN)
        if "player_abbreviations" in kwargs and kwargs["player_abbreviations"] is not None:
            paqm = QueryModel()
            for pabb in kwargs["player_abbreviations"]:
                if not (pabb is None or pabb == ""):
                    curqm = QueryModel()
                    fi, ln = util.parseNameAbbreviation(pabb)
                    if not (fi is None and ln is None):
                        if fi is not None:
                            curqm.cstart("first_name", fi, Operator.REGEX, "i")
                        curqm.cand("last_name", ln, Operator.REGEX, "i")
                        paqm.cor(query_model=curqm)
            qm.cand(query_model=paqm)
        return qm

    async def _setPreviousTeams(self, rosters : List[dict]) -> List[dict]:
        # Get the current rosters
        crosters = await self.find()
        if len(crosters) > 0:
            npmap = __makeProfileIdMap__(rosters)
            cpmap = __makeProfileIdMap__(crosters)
            for pid in npmap:
                if pid in cpmap:
                    # profile_id is in the current data
                    if cpmap[pid]["team"] != npmap[pid]["team"]:
                        # The team value for the new data is different than
                        # the current data, therefore, we need to add the
                        # current data team as a previous_team for this player
                        pteams = [cpmap[pid]["team"]]
                        if "previous_teams" in cpmap[pid]["team"]:
                            pteams.extend(cpmap[pid]["team"]["previous_teams"])
                            pteams = list(set(pteams))
                        npmap[pid]["previous_teams"] = pteams
        return rosters

    def _getHistoricData(self) -> List[dict]:
        hddir = os.path.join(os.path.dirname(__file__), "..", "..", "data")
        hdfile = os.path.join(hddir, "historic_roster.json")
        data = None
        if os.path.exists(hdfile):
            with open(hdfile, "rt") as fp:
                data = json.load(fp)
        return data

def __makeProfileIdMap__(records : List[dict]) -> dict:
    pids = [_["profile_id"] for _ in records]
    return dict(zip(pids, records))