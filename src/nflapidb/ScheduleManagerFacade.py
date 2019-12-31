from typing import List
import datetime
import nflapi.Client
from nflapidb.EntityManager import EntityManager
from nflapidb.DataManagerFacade import DataManagerFacade
from nflapidb.QueryModel import QueryModel, Operator
from nflapidb.SeasonTypeSequence import SeasonTypeSequence
import nflapidb.Utilities as util

class ScheduleManagerFacade(DataManagerFacade):

    def __init__(self, entityManager : EntityManager,
                 apiClient : nflapi.Client.Client = None):
        super(ScheduleManagerFacade, self).__init__("schedule", entityManager, apiClient)
        self._proc_ent_name = "schedule_process"
        self._season_type_seq = SeasonTypeSequence()
        self._last_process_data = None
        self._min_process_data = None
        self._min_season = 2017
        self._min_sync_season = None
        self._min_sync_season_type = None
        self._min_sync_week = None

    async def sync(self, all : bool = False) -> List[dict]:
        schedules = []
        for season in range(await self._minSyncSeason(), util.getSeason() + 1):
            schedule = self._apiClient.getSchedule(season=season)
            schedules.extend(await self.save(schedule))
        return schedules

    async def save(self, data : List[dict]) -> List[dict]:
        if len(data) > 0:
            data = await super(ScheduleManagerFacade, self).save(data)
        return data

    async def find(self, teams : List[str] = None,
                   seasons : List[int] = None,
                   season_types : List[str] = None,
                   weeks : List[int] = None,
                   last : bool = False,
                   next : bool = False) -> List[dict]:
        if last:
            seasons = [await self._findLastSeason()]
            season_types = [await self._findLastSeasonType()]
            weeks = [await self._findLastWeek()]
        elif next:
            seasons = [await self._findNextSeason()]
            season_types = [await self._findNextSeasonType()]
            weeks = [await self._findNextWeek()]
        return await super(ScheduleManagerFacade, self).find(teams=teams,
                                                             seasons=seasons,
                                                             season_types=season_types,
                                                             weeks=weeks)

    async def delete(self, teams : List[str] = None,
                     seasons : List[int] = None,
                     season_types : List[str] = None,
                     weeks : List[int] = None) -> List[dict]:
        return await super(ScheduleManagerFacade, self).delete(teams=teams,
                                                               seasons=seasons,
                                                               season_types=season_types,
                                                               weeks=weeks)

    def _getQueryModel(self, **kwargs) -> QueryModel:
        qm = QueryModel()
        if kwargs["teams"] is not None:
            qm.cstart("teams", kwargs["teams"], Operator.IN)
        cmap = {
            "season": kwargs["seasons"] if "seasons" in kwargs else None,
            "season_type": kwargs["season_types"] if "season_types" in kwargs else None,
            "week": kwargs["weeks"] if "weeks" in kwargs else None
        }
        for name in cmap:
            if cmap[name] is not None:
                qm.cand(name, cmap[name], Operator.IN)
        return qm

    async def _findLastSeason(self) -> int:
        pd = await self._lastProcessData()
        return pd["season"] if pd is not None else None

    async def _findNextSeason(self) -> int:
        pd = await self._minProcessData()
        return pd["season"]

    async def _findLastSeasonType(self) -> str:
        pd = await self._lastProcessData()
        return pd["season_type"] if pd is not None else None

    async def _findNextSeasonType(self) -> str:
        pd = await self._minProcessData()
        return pd["season_type"]

    async def _findLastWeek(self) -> int:
        pd = await self._lastProcessData()
        return pd["week"] if pd is not None else None

    async def _findNextWeek(self) -> int:
        pd = await self._minProcessData()
        return pd["week"]

    async def _lastProcessData(self) -> dict:
        if self._last_process_data is None:
            curpdata = await self._entityManager.find(self._proc_ent_name)
            if len(curpdata) > 0:
                self._last_process_data = curpdata[0]
        return self._last_process_data

    async def _minProcessData(self) -> dict:
        if self._min_process_data is None:
            lpdata = await self._lastProcessData()
            if lpdata is None:
                lpdata = {
                    "season": self._min_season,
                    "season_type": self._season_type_seq.min,
                    "week": 0
                }
            else:
                if lpdata["week"] == 22:
                    # We last processed the last week of postseason
                    # so next update should be the first week of
                    # the next preseason
                    lpdata["season"] += 1
                    lpdata["season_type"] = self._season_type_seq.min
                    lpdata["week"] = 0
                elif lpdata["week"] == 4 and lpdata["season_type"] == self._season_type_seq.min:
                    # We last processed the last week of preseason
                    # so next update should be the first week of
                    # the regular_season
                    self._season_type_seq.current = lpdata["season_type"]
                    lpdata["season_type"] = self._season_type_seq.next
                    lpdata["week"] = 1
                elif lpdata["week"] == 17:
                    # We last processed the last week of regular_season
                    # so next update should be the first week of
                    # the postseason
                    self._season_type_seq.current = lpdata["season_type"]
                    lpdata["season_type"] = self._season_type_seq.next
                    lpdata["week"] = 1
                else:
                    # We last processed an intraweek of the recorded
                    # season_type so we just need to increment
                    lpdata["week"] += 1
            self._min_process_data = lpdata
        return self._min_process_data

    async def _minSyncSeason(self) -> int:
        if self._min_sync_season is None:
            lpdata = await self._minProcessData()
            self._min_sync_season = lpdata["season"]
        return self._min_sync_season

    async def _minSyncSeasonType(self) -> int:
        if self._min_sync_season_type is None:
            lpdata = await self._minProcessData()
            self._min_sync_season_type = lpdata["season_type"]
        return self._min_sync_season_type

    async def _minSyncWeek(self) -> int:
        if self._min_sync_week is None:
            lpdata = await self._minProcessData()
            self._min_sync_week = lpdata["week"]
        return self._min_sync_week
