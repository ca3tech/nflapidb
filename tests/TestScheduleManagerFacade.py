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

    def test_sync_req_unfinished_only_after_initialized_in_regseason(self):
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
        smgr = self._getMockScheduleManager(scheduleData=srcdata)
        recs1 = util.runCoroutine(smgr.sync())
        self.assertEqual(len(recs1), len(srcdata), "sync1 returned record count differs")
        smgr = self._getMockScheduleManager(scheduleData=srcdata)
        recs2 = util.runCoroutine(smgr.sync())
        self.assertEqual(len(recs2), len(updata), "sync2 returned record count differs")
        dbrecs = util.runCoroutine(self.entmgr.find(self.entityName))
        self.assertEqual(dbrecs, recs1, "db records differ")
        apireq = smgr._apiClient.getRequestedData()
        self.assertEqual(len(apireq), len(xreq), "api request lengths differ")
        self.assertEqual(apireq, xreq, "api requests differ")

    def test_sync_req_postseason_only_after_initialized_after_regseason(self):
        srcdata = []
        psdata = []
        tddpath = os.path.join(os.path.dirname(__file__), "data")
        for fname in ["schedule_2017.json", "schedule_2018.json", "schedule_2019.json"]:
            with open(os.path.join(tddpath, fname), "rt") as fp:
                data = json.load(fp)
                srcdata.extend(data)
                if fname == "schedule_2018.json":
                    # use the 2018 postseason data as the 2019 postseason data
                    for d in [r for r in data if r["season_type"] == "postseason"]:
                        ud = d.copy()
                        ud["finished"] = False
                        ud["season"] = 2019
                        ud["gsis_id"] = ud["gsis_id"].replace("2019", "2020", 1)
                        psdata.append(ud)
        for rec in srcdata:
            if not rec["finished"]:
                rec["finished"] = True
        xreq = []
        for week in range(1, 5):
            xreq.append({"season": 2019, "season_type": "postseason", "week": week})
        smgr = self._getMockScheduleManager(scheduleData=srcdata)
        recs1 = util.runCoroutine(smgr.sync())
        self.assertEqual(len(recs1), len(srcdata), "sync1 returned record count differs")
        smgr = self._getMockScheduleManager(scheduleData=psdata)
        recs2 = util.runCoroutine(smgr.sync())
        self.assertEqual(len(recs2), len(psdata), "sync2 returned record count differs")
        dbrecs = util.runCoroutine(self.entmgr.find(self.entityName))
        xdbrecs = recs1 + recs2
        self.assertEqual(len(dbrecs), len(xdbrecs), "db record lengths differ")
        self.assertEqual(dbrecs, xdbrecs, "db records differ")
        apireq = smgr._apiClient.getRequestedData()
        self.assertEqual(len(apireq), len(xreq), "api request lengths differ")
        self.assertEqual(apireq, xreq, "api requests differ")

    def test_save_adds_teams(self):
        with open(os.path.join(os.path.dirname(__file__), "data", "schedule_2017.json"), "rt") as fp:
            srcdata = json.load(fp)
        d = srcdata[0]
        smgr = ScheduleManagerFacade(self.entmgr)
        recs = util.runCoroutine(smgr.save([d]))
        self.assertEqual(len(recs), 1, "returned record count differs")
        self.assertTrue("teams" in recs[0], "teams not added")
        dbrecs = util.runCoroutine(self.entmgr.find(self.entityName))
        self.assertEqual(dbrecs, recs, "db records differ")
        self.assertEqual(dbrecs[0]["teams"], ["MIA", "ATL"], "teams value differs")

    def test_find_by_teams(self):
        with open(os.path.join(os.path.dirname(__file__), "data", "schedule_2017.json"), "rt") as fp:
            srcdata = json.load(fp)
        smgr = self._getMockScheduleManager(scheduleData=srcdata)
        util.runCoroutine(smgr.sync())
        recs = util.runCoroutine(smgr.find(teams=["ATL"]))
        self.assertEqual(len(recs), 22, "ATL record count differs")
        recs = util.runCoroutine(smgr.find(teams=["ATL", "NE"]))
        self.assertEqual(len(recs), 44, "ATL, NE record count differs")

    def test_find_by_finished(self):
        with open(os.path.join(os.path.dirname(__file__), "data", "schedule_2019.json"), "rt") as fp:
            srcdata = json.load(fp)
        smgr = self._getMockScheduleManager(scheduleData=srcdata)
        util.runCoroutine(smgr.sync())
        recs = util.runCoroutine(smgr.find(finished=True))
        self.assertEqual(len(recs), 32, "True record count differs")
        recs = util.runCoroutine(smgr.find(finished=False))
        self.assertEqual(len(recs), 8, "False record count differs")

    def test_find_by_season(self):
        srcdata = []
        tddpath = os.path.join(os.path.dirname(__file__), "data")
        for fname in ["schedule_2017.json", "schedule_2018.json", "schedule_2019.json"]:
            with open(os.path.join(tddpath, fname), "rt") as fp:
                srcdata.extend(json.load(fp))
        smgr = self._getMockScheduleManager(scheduleData=srcdata)
        util.runCoroutine(smgr.sync())
        recs = util.runCoroutine(smgr.find(seasons=[2017]))
        self.assertEqual(len(recs), 44, "2017 record count differs")
        recs = util.runCoroutine(smgr.find(seasons=[2017, 2019]))
        self.assertEqual(len(recs), 84, "2017, 2019 record count differs")

    def test_find_by_season_type(self):
        with open(os.path.join(os.path.dirname(__file__), "data", "schedule_2017.json"), "rt") as fp:
            srcdata = json.load(fp)
        smgr = self._getMockScheduleManager(scheduleData=srcdata)
        util.runCoroutine(smgr.sync())
        recs = util.runCoroutine(smgr.find(season_types=["regular_season"]))
        self.assertEqual(len(recs), 31, "regular_season record count differs")
        recs = util.runCoroutine(smgr.find(season_types=["regular_season", "postseason"]))
        self.assertEqual(len(recs), 36, "regular_season, postseason record count differs")

    def test_find_by_season_type_week(self):
        with open(os.path.join(os.path.dirname(__file__), "data", "schedule_2017.json"), "rt") as fp:
            srcdata = json.load(fp)
        smgr = self._getMockScheduleManager(scheduleData=srcdata)
        util.runCoroutine(smgr.sync())
        recs = util.runCoroutine(smgr.find(season_types=["regular_season"], weeks=[1]))
        self.assertEqual(len(recs), 2, "regular_season week 1 record count differs")
        recs = util.runCoroutine(smgr.find(season_types=["postseason"], weeks=[2]))
        self.assertEqual(len(recs), 2, "postseason week 2 record count differs")

    def test_find_by_last(self):
        srcdata = []
        tddpath = os.path.join(os.path.dirname(__file__), "data")
        for fname in ["schedule_2017.json", "schedule_2018.json", "schedule_2019.json"]:
            with open(os.path.join(tddpath, fname), "rt") as fp:
                srcdata.extend(json.load(fp))
        smgr = self._getMockScheduleManager(scheduleData=srcdata)
        util.runCoroutine(smgr.sync())
        recs = util.runCoroutine(smgr.find(last=True))
        self.assertEqual(len(recs), 2, "record count differs")
        for rec in recs:
            self.assertEqual(rec["season"], 2019, "{} season differs".format(rec["gsis_id"]))
            self.assertEqual(rec["season_type"], "regular_season", "{} season_type differs".format(rec["gsis_id"]))
            self.assertEqual(rec["week"], 13, "{} week differs".format(rec["gsis_id"]))

    def test_find_by_next(self):
        srcdata = []
        tddpath = os.path.join(os.path.dirname(__file__), "data")
        for fname in ["schedule_2017.json", "schedule_2018.json", "schedule_2019.json"]:
            with open(os.path.join(tddpath, fname), "rt") as fp:
                srcdata.extend(json.load(fp))
        smgr = self._getMockScheduleManager(scheduleData=srcdata)
        util.runCoroutine(smgr.sync())
        recs = util.runCoroutine(smgr.find(next=True))
        self.assertEqual(len(recs), 2, "record count differs")
        for rec in recs:
            self.assertEqual(rec["season"], 2019, "{} season differs".format(rec["gsis_id"]))
            self.assertEqual(rec["season_type"], "regular_season", "{} season_type differs".format(rec["gsis_id"]))
            self.assertEqual(rec["week"], 14, "{} week differs".format(rec["gsis_id"]))

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
        rqwk = week
        if week is not None:
            # even though the nflapi Client.getSchedule method takes
            # week values in the range 1-4 for the postseason it
            # actually returns the week as returned by nfl.com,
            # which has the range 18-22 with 21 missing, therefore,
            # when I query the input data I need to map the week value
            if season_type == "postseason":
                rqwk = week + 17
            if rqwk == 21:
                rqwk += 1
            reqdata["week"] = week
        self._req_data.append(reqdata)
        data : List[dict] = []
        for rec in self._roster_data:
            keep = season is None or rec["season"] == season
            keep = keep and (season_type is None or rec["season_type"] == season_type)
            keep = keep and (rqwk is None or rec["week"] == rqwk)
            if keep:
                data.append(rec)
        return data

    def getRequestedData(self) -> List[dict]:
        return self._req_data
