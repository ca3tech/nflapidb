from typing import List
import logging
import nflapi.Client
from nflapidb.EntityManager import EntityManager
from nflapidb.DataManagerFacade import DataManagerFacade
from nflapidb.QueryModel import QueryModel, Operator
from nflapidb.RosterManagerFacade import RosterManagerFacade
import nflapidb.Utilities as util

class PlayerProfileManagerFacade(DataManagerFacade):

    def __init__(self, entityManager : EntityManager,
                 apiClient : nflapi.Client.Client = None,
                 rosterManager : RosterManagerFacade = None):
        super(PlayerProfileManagerFacade, self).__init__("player_profile", entityManager, apiClient)
        self._rmgr = rosterManager

    async def sync(self, all : bool = False) -> List[dict]:
        logging.info("Syncing player profile data...")
        rmgr = self._rosterManager
        recs = await rmgr.find()
        recs = await self._filterUnchangedRosters(recs, all)
        logging.info("Retrieving player profiles from NFL API...")
        return await self.save(self._addRosterData(self._apiClient.getPlayerProfile(recs), recs))

    async def save(self, data : List[dict]) -> List[dict]:
        logging.info("Saving player profile data...")
        if len(data) > 0:
            cdata = await self._setPreviousTeams(data)
            data = await super(PlayerProfileManagerFacade, self).save(cdata)
        return data

    async def find(self, teams : List[str] = None,
                   positions : List[str] = None,
                   last_names : List[str] = None,
                   first_names : List[str] = None,
                   profile_ids : List[int] = None,
                   include_previous_teams : bool = False) -> List[dict]:
        return await super(PlayerProfileManagerFacade, self).find(teams=teams, positions=positions,
                                                                  last_names=last_names,
                                                                  first_names=first_names,
                                                                  profile_ids=profile_ids,
                                                                  include_previous_teams=include_previous_teams)

    async def delete(self, teams : List[str] = None,
                     profile_ids : List[int] = None) -> List[dict]:
        return await super(PlayerProfileManagerFacade, self).delete(teams=teams,
                                                                    profile_ids=profile_ids)

    def _addRosterData(self, profdata : List[dict], rostdata : List[dict]) -> List[dict]:
        if profdata is not None and rostdata is not None and len(profdata) > 0 and len(rostdata) > 0:
            rostidx = dict([(rostdata[i]["profile_id"], i) for i in range(0, len(rostdata))])
            for pp in profdata:
                pid = pp["profile_id"]
                if pid in rostidx:
                    r = rostdata[rostidx[pid]]
                    if "previous_teams" in r:
                        pp["previous_teams"] = r["previous_teams"]
        return profdata

    @property
    def _rosterManager(self) -> RosterManagerFacade:
        if self._rmgr is None:
            self._rmgr = RosterManagerFacade(self._entityManager, self._apiClient)
        return self._rmgr

    def _getQueryModel(self, **kwargs) -> QueryModel:
        qm = QueryModel()
        if kwargs["teams"] is not None:
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

    async def _filterUnchangedRosters(self, recs : List[dict], all : bool) -> List[dict]:
        frecs = recs
        if not all:
            rpidmap = __makeProfileIdMap__(recs)
            rpids = list(rpidmap.keys())
            cpprecs = await self.find(profile_ids=rpids)
            if len(cpprecs) > 0:
                cpidmap = __makeProfileIdMap__(cpprecs)
                frecs = []
                for pid in rpids:
                    if pid in cpidmap:
                        rrec = rpidmap[pid]
                        crec = cpidmap[pid]
                        if rrec["team"] != crec["team"]:
                            frecs.append(rpidmap[pid])
                    else:
                        frecs.append(rpidmap[pid])
        return frecs

def __makeProfileIdMap__(records : List[dict]) -> dict:
    pids = [_["profile_id"] for _ in records]
    return dict(zip(pids, records))