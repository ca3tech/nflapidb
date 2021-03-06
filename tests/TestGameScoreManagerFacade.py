import unittest
import os
import json
from typing import List
import nflapi.Client
from nflapidb.ScheduleManagerFacade import ScheduleManagerFacade
from nflapidb.RosterManagerFacade import RosterManagerFacade
from nflapidb.GameScoreManagerFacade import GameScoreManagerFacade
from nflapidb.EntityManager import EntityManager
import nflapidb.Utilities as util
from nflapidb.QueryModel import QueryModel

class TestGameScoreManagerFacade(unittest.TestCase):

    def setUp(self):
        self.entityName = "game_score"
        self.entmgr = EntityManager()
        self.rostmgr = None

    def tearDown(self):
        util.runCoroutine(self.entmgr.drop(self.entityName))
        if self.rostmgr is not None:
            util.runCoroutine(self.entmgr.drop(self.rostmgr.entityName))
        self.entmgr.dispose()

    def _getMockGameScoreManager(self, scheduleData : List[dict],
                                 gmscrData : List[dict]):
        apiClient = MockApiClient(gmscrData)
        schmgr = MockScheduleManagerFacade(self.entmgr, scheduleData)
        self.datamgr = GameScoreManagerFacade(self.entmgr, apiClient, schmgr)
        return self.datamgr

    def _getTestDataPath(self, fname : str = None) -> str:
        path = os.path.join(os.path.dirname(__file__), "data")
        if fname is not None:
            path = os.path.join(path, fname)
        return path

    def _getScheduleData(self, weeks : List[int] = None) -> List[dict]:
        if weeks is None:
            weeks = [13, 14]
        with open(self._getTestDataPath("schedule_2019.json"), "rt") as fp:
            return [r for r in json.load(fp) if r["week"] in weeks]

    def _getGameScoreData(self, weeks : List[int] = None) -> List[dict]:
        if weeks is None:
            weeks = [13, 14]
        data = []
        for week in weeks:
            with open(self._getTestDataPath(f"game_score_2019_reg_{week}.json"), "rt") as fp:
                data.extend(json.load(fp))
        return data

    def test_sync_initializes_collection(self):
        schdata = self._getScheduleData()
        gsdata = self._getGameScoreData()
        gsmgr = self._getMockGameScoreManager(schdata, gsdata)
        recs = util.runCoroutine(gsmgr.sync())
        self.assertEqual(len(recs), len(gsdata), "returned record counts differ")
        dbrecs = util.runCoroutine(gsmgr.find())
        for r in recs:
            del r["_id"]
        self.assertEqual(len(dbrecs), len(recs), "db record counts differ")
        self.assertEqual(dbrecs, recs, "db records differ")
        apireqs = gsmgr._apiClient.getRequestedSchedules()
        self.assertEqual(len(apireqs), len(schdata), "api request record counts differ")
        self.assertEqual(apireqs, schdata, "api request records differ")

    def test_sync_no_new_finished_noop(self):
        schdata = self._getScheduleData()
        gsdata = self._getGameScoreData([13])
        gsmgr = self._getMockGameScoreManager(schdata, gsdata)
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
        gsdata1 = self._getGameScoreData([13])
        gsdata2 = self._getGameScoreData([14])
        gsmgr = self._getMockGameScoreManager(schdata1, gsdata1)
        recs1 = util.runCoroutine(gsmgr.sync())
        self.assertEqual(len(recs1), len(gsdata1), "first returned record counts differ")
        gsmgr = self._getMockGameScoreManager(schdata2, gsdata2)
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
    def __init__(self, gmscrData : List[dict]):
        self._gmscr_data = {}
        for r in gmscrData:
            gsid = r["gsis_id"]
            if gsid not in self._gmscr_data:
                self._gmscr_data[gsid] = []
            self._gmscr_data[gsid].append(r)
        self._req_schedules = []

    def getGameScore(self, schedules : List[str]) -> List[dict]:
        self._req_schedules = schedules
        data = []
        for gsid in [_["gsis_id"] for _ in schedules]:
            if gsid in self._gmscr_data:
                data.extend(self._gmscr_data[gsid])
        return data

    def getRequestedSchedules(self) -> List[dict]:
        return self._req_schedules
