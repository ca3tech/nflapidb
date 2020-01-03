import unittest
import os
import json
from typing import List
import nflapi.Client
from nflapidb.ScheduleManagerFacade import ScheduleManagerFacade
from nflapidb.RosterManagerFacade import RosterManagerFacade
from nflapidb.GameSummaryManagerFacade import GameSummaryManagerFacade
from nflapidb.EntityManager import EntityManager
import nflapidb.Utilities as util
from nflapidb.QueryModel import QueryModel

class TestGameSummaryManagerFacade(unittest.TestCase):

    def setUp(self):
        self.entityName = "game_summary"
        self.entmgr = EntityManager()
        self.rostmgr = None

    def tearDown(self):
        util.runCoroutine(self.entmgr.drop(self.entityName))
        if self.rostmgr is not None:
            util.runCoroutine(self.entmgr.drop(self.rostmgr.entityName))
        self.entmgr.dispose()

    def _getMockGameSummaryManager(self, scheduleData : List[dict],
                                   gmsumData : List[dict]):
        apiClient = MockApiClient(gmsumData)
        schmgr = MockScheduleManagerFacade(self.entmgr, scheduleData)
        self.rostmgr = MockRosterManagerFacade(self.entmgr, self._getRosterData())
        self.datamgr = GameSummaryManagerFacade(self.entmgr, apiClient, schmgr, self.rostmgr)
        return self.datamgr

    def _getTestDataPath(self, fname : str = None) -> str:
        path = os.path.join(os.path.dirname(__file__), "data")
        if fname is not None:
            path = os.path.join(path, fname)
        return path

    def _getRosterData(self, teams : List[str] = None) -> List[dict]:
        if teams is None:
            teams = ["ari", "hou", "kc", "la", "ne", "sea"]
        rdata = []
        tddir = self._getTestDataPath()
        for t in teams:
            t = t.lower()
            fpath = os.path.join(tddir, f"roster_{t}.json")
            with open(fpath, "rt") as fp:
                rdata.extend(json.load(fp))
        return rdata

    def _getScheduleData(self, weeks : List[int] = None) -> List[dict]:
        if weeks is None:
            weeks = [13, 14]
        with open(self._getTestDataPath("schedule_2019.json"), "rt") as fp:
            return [r for r in json.load(fp) if r["week"] in weeks]

    def _getGameSummaryData(self, weeks : List[int] = None) -> List[dict]:
        if weeks is None:
            weeks = [13, 14]
        data = []
        for week in weeks:
            with open(self._getTestDataPath(f"game_summary_2019_reg_{week}.json"), "rt") as fp:
                data.extend(json.load(fp))
        return data

    def test_sync_initializes_collection(self):
        schdata = self._getScheduleData()
        gsdata = self._getGameSummaryData()
        gsmgr = self._getMockGameSummaryManager(schdata, gsdata)
        recs = util.runCoroutine(gsmgr.sync())
        self.assertEqual(len(recs), len(gsdata), "returned record counts differ")
        dbrecs = util.runCoroutine(gsmgr.find())
        for r in recs:
            del r["_id"]
        self.assertEqual(len(dbrecs), len(recs), "db record counts differ")
        self.assertEqual(dbrecs, recs, "db records differ")
        self.assertGreater(len([r for r in dbrecs if "profile_id" in r]), 0, "no profile_ids added")
        apireqs = gsmgr._apiClient.getRequestedSchedules()
        self.assertEqual(len(apireqs), len(schdata), "api request record counts differ")
        self.assertEqual(apireqs, schdata, "api request records differ")

    def test_sync_no_new_finished_noop(self):
        schdata = self._getScheduleData()
        gsdata = self._getGameSummaryData([13])
        gsmgr = self._getMockGameSummaryManager(schdata, gsdata)
        recs1 = util.runCoroutine(gsmgr.sync())
        self.assertEqual(len(recs1), len(gsdata), "first returned record counts differ")
        recs2 = util.runCoroutine(gsmgr.sync())
        self.assertEqual(len(recs2), 0, "second returned record counts differ")
        dbrecs = util.runCoroutine(gsmgr.find())
        self.assertEqual(len(dbrecs), len(recs1), "db record counts differ")

    def test_sync_new_finished_adds(self):
        schdata1 = self._getScheduleData()
        schdata2 = schdata1.copy()
        reqsch2 = []
        for r in schdata2:
            if not r["finished"]:
                r["finished"] = True
                reqsch2.append(r)
        gsdata1 = self._getGameSummaryData([13])
        gsdata2 = self._getGameSummaryData([14])
        gsmgr = self._getMockGameSummaryManager(schdata1, gsdata1)
        recs1 = util.runCoroutine(gsmgr.sync())
        self.assertEqual(len(recs1), len(gsdata1), "first returned record counts differ")
        gsmgr = self._getMockGameSummaryManager(schdata2, gsdata2)
        recs2 = util.runCoroutine(gsmgr.sync())
        self.assertEqual(len(recs2), len(gsdata2), "second returned record counts differ")
        dbrecs = util.runCoroutine(gsmgr.find())
        recs = recs1 + recs2
        for r in recs:
            del r["_id"]
        self.assertEqual(len(dbrecs), len(recs), "db record counts differ")
        self.assertEqual(dbrecs, recs, "db records differ")
        apireqs = gsmgr._apiClient.getRequestedSchedules()
        self.assertEqual(len(apireqs), len(reqsch2), "api request record counts differ")
        self.assertEqual(apireqs, reqsch2, "api request records differ")
    
class MockRosterManagerFacade(RosterManagerFacade):
    def __init__(self, entityManager : EntityManager,
                 findData : List[dict]):
        super(MockRosterManagerFacade, self).__init__(entityManager)
        util.runCoroutine(self.save(findData))

class MockScheduleManagerFacade(ScheduleManagerFacade):
    def __init__(self, entityManager : EntityManager,
                 findData : List[dict]):
        super(MockScheduleManagerFacade, self).__init__(entityManager)
        self._find_data = findData

    async def find(self, qm : QueryModel) -> List[dict]:
        if qm is None:
            return self._find_data
        else:
            cnst = qm.constraint
            def getop(d):
                return list(d)[0]
            def isin(tval, xvals):
                return tval in xvals
            def isnin(tval, xvals):
                return tval not in xvals
            def iseq(tval, xval):
                return tval == xval
            def isneq(tval, xval):
                return tval != xval
            def getTestOp(d):
                fd = {
                    "$in": isin,
                    "$nin": isnin,
                    "$eq": iseq,
                    "$neq": isneq
                }
                op = getop(d)
                return fd[op]
            gsid = fgsid = fin = ffin = None
            for d in cnst["$and"]:
                if "gsis_id" in d:
                    vd = d["gsis_id"]
                    k = getop(vd)
                    gsid = vd[k]
                    fgsid = getTestOp(vd)
                elif "finished" in d:
                    vd = d["finished"]
                    k = getop(vd)
                    fin = vd[k]
                    ffin = getTestOp(vd)
            return [r for r in self._find_data if fgsid(r["gsis_id"], gsid) and ffin(r["finished"], fin)]

class MockApiClient(nflapi.Client.Client):
    def __init__(self, gmsumData : List[dict]):
        self._gmsum_data = {}
        for r in gmsumData:
            gsid = r["gsis_id"]
            if gsid not in self._gmsum_data:
                self._gmsum_data[gsid] = []
            self._gmsum_data[gsid].append(r)
        self._req_schedules = []

    def getGameSummary(self, schedules : List[str]) -> List[dict]:
        self._req_schedules = schedules
        data = []
        for gsid in [_["gsis_id"] for _ in schedules]:
            if gsid in self._gmsum_data:
                data.extend(self._gmsum_data[gsid])
        return data

    def getRequestedSchedules(self) -> List[dict]:
        return self._req_schedules
