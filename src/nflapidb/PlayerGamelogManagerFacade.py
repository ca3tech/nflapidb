from typing import List
import datetime
import nflapi.Client
from nflapidb.EntityManager import EntityManager
from nflapidb.DataManagerFacade import DataManagerFacade
from nflapidb.QueryModel import QueryModel, Operator
from nflapidb.RosterManagerFacade import RosterManagerFacade
import nflapidb.Utilities as util

class PlayerGamelogManagerFacade(DataManagerFacade):

    def __init__(self, entityManager : EntityManager,
                 apiClient : nflapi.Client.Client = None,
                 rosterManager : RosterManagerFacade = None):
        super(PlayerGamelogManagerFacade, self).__init__("player_gamelog", entityManager, apiClient)
        self._rmgr = rosterManager
        self._plr_gl_proc_ent_name = "player_gamelog_process"
        self._process_date = datetime.datetime.today()
        self._min_season = 2017
        self._min_sync_season = None
        self._current_season = None
        self._last_process_date = None

    async def sync(self, all : bool = False) -> List[dict]:
        rmgr = self._rosterManager
        recs = await rmgr.find()
        if not (all or await self._isDataExpired()):
            recs = await self._filterUnchangedRosters(recs)
            all = True
        gl = []
        if len(recs) > 0:
            if all:
                mnseason = self._min_season
            else:
                mnseason = await self._minSyncSeason()
            mxseason = self._currentSeason
            for season in range(mnseason, mxseason + 1):
                gl.extend(self._apiClient.getPlayerGameLog(rosters=recs, season=season))
            await self.save(gl)
            await self._updateDataExpired()
        return gl

    async def save(self, data : List[dict]) -> List[dict]:
        if len(data) > 0:
            cdata = await self._setPreviousTeams(data)
            data = await super(PlayerGamelogManagerFacade, self).save(cdata)
        return data

    async def find(self, teams : List[str] = None,
                   last_names : List[str] = None,
                   first_names : List[str] = None,
                   profile_ids : List[int] = None,
                   include_previous_teams : bool = False) -> List[dict]:
        return await super(PlayerGamelogManagerFacade, self).find(teams=teams,
                                                                  last_names=last_names,
                                                                  first_names=first_names,
                                                                  profile_ids=profile_ids,
                                                                  include_previous_teams=include_previous_teams)

    async def delete(self, teams : List[str] = None,
                     profile_ids : List[int] = None) -> List[dict]:
        return await super(PlayerGamelogManagerFacade, self).delete(teams=teams,
                                                                    profile_ids=profile_ids)

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
            "last_name": kwargs["last_names"] if "last_names" in kwargs else None,
            "first_name": kwargs["first_names"] if "first_names" in kwargs else None,
            "profile_id": kwargs["profile_ids"] if "profile_ids" in kwargs else None
        }
        for name in cmap:
            if cmap[name] is not None:
                qm.cand(name, cmap[name], Operator.IN)
        return qm

    async def _setPreviousTeams(self, gmlogs : List[dict]) -> List[dict]:
        # Get the current game logs
        cgmlogs = await self.find()
        if len(cgmlogs) > 0:
            npmap = __makeProfileIdMap__(gmlogs)
            cpmap = __makeProfileIdMap__(cgmlogs)
            for pid in npmap:
                if pid in cpmap:
                    # profile_id is in the current data
                    nrecs = npmap[pid]
                    crecs = cpmap[pid]
                    nteams = set([_["team"] for _ in nrecs])
                    cteams = set([_["team"] for _ in crecs])
                    if cteams != nteams:
                        # The team value for the new data is different than
                        # the current data, therefore, we need to add the
                        # current data team as a previous_team for this player
                        pteams = list(cteams)
                        for rec in crecs:
                            if "previous_teams" in rec:
                                pteams.extend(rec["previous_teams"])
                                pteams = list(set(pteams))
                        for rec in nrecs:
                            rec["previous_teams"] = pteams
        return gmlogs

    async def _filterUnchangedRosters(self, recs : List[dict]) -> List[dict]:
        frecs = recs
        rpidmap = __makeProfileIdMap__(recs)
        rpids = list(rpidmap.keys())
        cpprecs = await self.find(profile_ids=rpids)
        if len(cpprecs) > 0:
            cpidmap = __makeProfileIdMap__(cpprecs)
            frecs = []
            for pid in rpids:
                if pid in cpidmap:
                    rrecs = rpidmap[pid]
                    crecs = cpidmap[pid]
                    rteams = set([_["team"] for _ in rrecs])
                    cteams = set([_["team"] for _ in crecs])
                    if rteams != cteams:
                        frecs.extend(rpidmap[pid])
                else:
                    frecs.extend(rpidmap[pid])
        return frecs

    def _getSeason(self, dt : datetime.datetime) -> int:
        return util.getSeason(dt)

    async def _minSyncSeason(self) -> int:
        if self._min_sync_season is None:
            lpdate = await self._lastProcessDate()
            if lpdate is not None:
                self._min_sync_season = self._getSeason(lpdate)
            else:
                self._min_sync_season = self._min_season
        return self._min_sync_season

    @property
    def _currentSeason(self) -> int:
        if self._current_season is None:
            self._current_season = self._getSeason(self._process_date)
        return self._current_season

    async def _lastProcessDate(self):
        if self._last_process_date is None:
            curpdates = await self._entityManager.find(self._plr_gl_proc_ent_name)
            if len(curpdates) > 0:
                self._last_process_date = curpdates[0]["process_date"]
        return self._last_process_date

    async def _isDataExpired(self):
        x = True
        curpdate = await self._lastProcessDate()
        if curpdate is not None:
            seyear = self._currentSeason + 1
            sedate = datetime.datetime(seyear, 3, 1)
            ssdate = datetime.datetime(seyear - 1, 8, 1)
            x = self._process_date > ssdate and self._process_date < sedate \
                and (self._process_date > curpdate + datetime.timedelta(weeks=1) \
                     or (curpdate.weekday() > 0 and self._process_date.weekday() == 1))
        return x

    async def _updateDataExpired(self, processDate : datetime.datetime = None):
        if processDate is None:
            processDate = self._process_date
        await self._entityManager.delete(self._plr_gl_proc_ent_name)
        await self._entityManager.save(self._plr_gl_proc_ent_name, [{"process_date": processDate}])

def __makeProfileIdMap__(records : List[dict]) -> dict:
    rmap = {}
    for rec in records:
        pid = rec["profile_id"]
        if pid in rmap:
            rmap[pid].append(rec)
        else:
            rmap[pid] = [rec]
    return rmap