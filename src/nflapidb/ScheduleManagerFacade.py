from typing import List
import datetime
import nflapi.Client
from nflapidb.EntityManager import EntityManager
from nflapidb.DataManagerFacade import DataManagerFacade
from nflapidb.QueryModel import QueryModel, Operator
import nflapidb.Utilities as util

class ScheduleManagerFacade(DataManagerFacade):

    def __init__(self, entityManager : EntityManager,
                 apiClient : nflapi.Client.Client = None):
        super(ScheduleManagerFacade, self).__init__("schedule", entityManager, apiClient)
        self._min_season = 2017

    async def sync(self, all : bool = False) -> List[dict]:
        schedules = []
        aqf = await self._getAPIQueryFilter()
        if aqf is not None:
            for f in aqf:
                schedule = self._apiClient.getSchedule(**f)
                schedules.extend(await self.save(schedule))
        return schedules

    async def save(self, data : List[dict]) -> List[dict]:
        if len(data) > 0:
            # add teams if not already exists
            for rec in data:
                if "teams" not in rec:
                    rec["teams"] = [rec["home_team"], rec["away_team"]]
            data = await super(ScheduleManagerFacade, self).save(data)
        return data

    async def find(self, qm : QueryModel = None,
                   teams : List[str] = None,
                   seasons : List[int] = None,
                   season_types : List[str] = None,
                   weeks : List[int] = None,
                   finished : bool = None,
                   last : bool = False,
                   next : bool = False) -> List[dict]:
        if qm is not None:
            recs = await super(ScheduleManagerFacade, self).find(qm=qm)
        elif last:
            recs = await self.find(finished=True)
            if len(recs) > 0:
                recs = self._filterLastWeek(recs)
        elif next:
            recs = await self.find(finished=False)
            if len(recs) > 0:
                recs = self._filterFirstWeek(recs)
        else:
            if finished is not None:
                finished = [finished]
            if weeks is not None and season_types is not None and season_types == ["postseason"]:
                # we allow postseason weeks to be specified either
                # in the range 1 to 4, or 18 to 22
                for i in range(0, len(weeks)):
                    if weeks[i] < 18:
                        if weeks[i] == 4:
                            weeks[i] = 22
                        else:
                            weeks[i] += 17
            recs = await super(ScheduleManagerFacade, self).find(teams=teams,
                                                                 seasons=seasons,
                                                                 season_types=season_types,
                                                                 weeks=weeks,
                                                                 finished=finished)
        return recs

    async def delete(self, teams : List[str] = None,
                     seasons : List[int] = None,
                     season_types : List[str] = None,
                     weeks : List[int] = None) -> List[dict]:
        return await super(ScheduleManagerFacade, self).delete(teams=teams,
                                                               seasons=seasons,
                                                               season_types=season_types,
                                                               weeks=weeks)

    async def _getAPIQueryFilter(self) -> List[dict]:
        ufdata = await self.find(finished=False)
        qf = None
        if len(ufdata) > 0:
            qf = []
            qd = {}
            for d in ufdata:
                cd = qd
                for k in ["season", "season_type", "week"]:
                    if not d[k] in cd:
                        cd[d[k]] = {}
                    cd = cd[d[k]]
            for s in qd:
                for st in qd[s]:
                    for w in qd[s][st]:
                        qf.append({"season": s, "season_type": st, "week": w})
        elif len(await self.find(finished=True)) == 0 or len(await self.find()) == 0:
            qf = [{"season": s} for s in range(self._min_season, util.getSeason() + 1)]
        elif len(await self.find(seasons=[util.getSeason()])) == 0:
            # We don't have data for the current season so we need to query it
            qf = [{"season": util.getSeason()}]
        else:
            s = util.getSeason()
            st = "postseason"
            psrecs = await self.find(seasons=[s], season_types=[st])
            if len(psrecs) < 11:
                # more postseason games to come
                qf = []
                wks = set([r["week"] for r in psrecs])
                mw = 1
                if len(wks) > 0:
                    mw = min(wks) + 1
                for w in range(mw, 5):
                    qf.append({"season": s, "season_type": st, "week": w})
        return qf

    def _getQueryModel(self, **kwargs) -> QueryModel:
        qm = QueryModel()
        if kwargs["teams"] is not None:
            qm.cstart("teams", kwargs["teams"], Operator.IN)
        cmap = {
            "season": kwargs["seasons"] if "seasons" in kwargs else None,
            "season_type": kwargs["season_types"] if "season_types" in kwargs else None,
            "week": kwargs["weeks"] if "weeks" in kwargs else None,
            "finished": kwargs["finished"] if "finished" in kwargs else None
        }
        for name in cmap:
            if cmap[name] is not None:
                qm.cand(name, cmap[name], Operator.IN)
        return qm

    def _filterLastWeek(self, recs : List[dict]) -> dict:
        recs = self._sortSchedules(recs)
        mrec = recs[len(recs)-1]
        return [r for r in recs if r["season"]==mrec["season"] and r["season_type"]==mrec["season_type"] and r["week"]==mrec["week"]]

    def _filterFirstWeek(self, recs : List[dict]) -> dict:
        recs = self._sortSchedules(recs)
        mrec = recs[0]
        return [r for r in recs if r["season"]==mrec["season"] and r["season_type"]==mrec["season_type"] and r["week"]==mrec["week"]]

    def _sortSchedules(self, recs : List[dict]) -> List[dict]:
        def skey(rec) -> float:
            return int(rec["gsis_id"])
        recs.sort(key=skey)
        return recs