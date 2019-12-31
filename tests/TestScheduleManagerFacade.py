import unittest
import os
import json
from typing import List
import nflapi.Client
from nflapidb.ScheduleManagerFacade import ScheduleManagerFacade
from nflapidb.EntityManager import EntityManager
import nflapidb.Utilities as util

class TestScheduleManagerFacade(unittest.TestCase):

    def setUp(self):
        self.entityName = "schedule"
        self.procEntityName = "schedule_process"
        self.entmgr = EntityManager()

    def tearDown(self):
        util.runCoroutine(self.entmgr.drop(self.entityName))
        util.runCoroutine(self.entmgr.drop(self.procEntityName))
        self.entmgr.dispose()

    def _getMockScheduleManager(self, scheduleData : List[dict]):
        apiClient = MockApiClient(scheduleData)
        self.datamgr = ScheduleManagerFacade(self.entmgr, apiClient)
        return self.datamgr

    def test_sync_initializes_collection(self):
        srcdata = []
        tddpath = os.path.join(os.path.dirname(__file__), "data")
        for fname in ["schedule_2017.json", "schedule_2018.json", "schedule_2019.json"]:
            with open(os.path.join(tddpath, fname), "rt") as fp:
                srcdata.extend(json.load(fp))
        xreq = [{"season": 2017}, {"season": 2018}, {"season": 2019}]
        smgr = self._getMockScheduleManager(scheduleData=srcdata)
        recs = util.runCoroutine(smgr.sync())
        self.assertEqual(len(recs), len(srcdata), "sync returned record count differs")
        dbrecs = util.runCoroutine(self.entmgr.find(self.entityName))
        self.assertEqual(dbrecs, recs, "db records differ")
        self.assertEqual(smgr._apiClient.getRequestedData(), xreq, "api requests differ")

    def test_sync_req_unfinished_only_after_initialized(self):
        srcdata = []
        updata = []
        tddpath = os.path.join(os.path.dirname(__file__), "data")
        for fname in ["schedule_2017.json", "schedule_2018.json", "schedule_2019.json"]:
            with open(os.path.join(tddpath, fname), "rt") as fp:
                data = json.load(fp)
                srcdata.extend(data)
                if fname == "schedule_2019.json":
                    updata = [rec for rec in data if not rec["finished"]]
        xreq = []
        for week in range(14, 18):
            xreq.append({"season": 2019, "season_type": "regular_season", "week": week})
        for week in range(1, 5):
            xreq.append({"season": 2019, "season_type": "postseason", "week": week})
        smgr = self._getMockScheduleManager(scheduleData=srcdata)
        recs1 = util.runCoroutine(smgr.sync())
        self.assertEqual(len(recs1), len(srcdata), "sync1 returned record count differs")
        smgr = self._getMockScheduleManager(scheduleData=srcdata)
        recs2 = util.runCoroutine(smgr.sync())
        self.assertEqual(len(recs2), len(updata), "sync2 returned record count differs")
        dbrecs = util.runCoroutine(self.entmgr.find(self.entityName))
        self.assertEqual(dbrecs, recs1, "db records differ")
        self.assertEqual(smgr._apiClient.getRequestedData(), xreq, "api requests differ")

class MockApiClient(nflapi.Client.Client):
    def __init__(self, scheduleData : List[dict]):
        self._roster_data = scheduleData
        self._req_data : List[dict] = []

    def getSchedule(self, season : int = None, season_type : str = None, week : int = None) -> List[dict]:
        reqdata = {}
        if season is not None:
            reqdata["season"] = season
        if season_type is not None:
            reqdata["season_type"] = season_type
        if week is not None:
            reqdata["week"] = week
        self._req_data.append(reqdata)
        data : List[dict] = []
        for rec in self._roster_data:
            keep = season is None or rec["season"] == season
            keep = keep and (season_type is None or rec["season_type"] == season_type)
            keep = keep and (week is None or rec["week"] == week)
            if keep:
                data.append(rec)
        return data

    def getRequestedData(self) -> List[dict]:
        return self._req_data
