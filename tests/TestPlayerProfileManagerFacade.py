import unittest
import os
import json
from typing import List
import random
import nflapi.Client
from nflapidb.RosterManagerFacade import RosterManagerFacade
from nflapidb.PlayerProfileManagerFacade import PlayerProfileManagerFacade
from nflapidb.EntityManager import EntityManager
import nflapidb.Utilities as util

class TestPlayerProfileManagerFacade(unittest.TestCase):

    def setUp(self):
        self.entityName = "player_profile"
        self.entmgr = EntityManager()

    def tearDown(self):
        util.runCoroutine(self.entmgr.drop(self.entityName))
        self.entmgr.dispose()

    def _getMockPlayerProfileManager(self, rosterData : List[dict], profileData : List[dict]):
        apiClient = MockApiClient(profileData)
        rmgr = MockRosterManagerFacade(self.entmgr, apiClient, rosterData)
        self.datamgr = PlayerProfileManagerFacade(self.entmgr, apiClient, rmgr)
        return self.datamgr

    def _getPlayerProfileManager(self):
        self.datamgr = PlayerProfileManagerFacade(self.entmgr)
        return self.datamgr

    def test_sync_initializes_collection_one_team(self):
        with open(os.path.join(os.path.dirname(__file__), "data", "roster_kc.json"), "rt") as fp:
            rstdata = json.load(fp)
        with open(os.path.join(os.path.dirname(__file__), "data", "player_profile_kc.json"), "rt") as fp:
            srcdata = json.load(fp)
        rmgr = self._getMockPlayerProfileManager(rosterData=rstdata, profileData=srcdata)
        recs = util.runCoroutine(rmgr.sync())
        self.assertEqual(len(recs), len(srcdata), "sync returned record count differs")
        dbrecs = util.runCoroutine(self.entmgr.find(self.entityName))
        self.assertEqual(dbrecs, recs, "db records differ")
        self.assertEqual(rmgr._apiClient.getRequestedRosters(), rstdata, "requested rosters differs")

    def test_sync_initializes_collection_two_teams(self):
        with open(os.path.join(os.path.dirname(__file__), "data", "roster_kc.json"), "rt") as fp:
            rstdata = json.load(fp)
        with open(os.path.join(os.path.dirname(__file__), "data", "roster_pit.json"), "rt") as fp:
            rstdata.extend(json.load(fp))
        with open(os.path.join(os.path.dirname(__file__), "data", "player_profile_kc.json"), "rt") as fp:
            srcdata = json.load(fp)
        with open(os.path.join(os.path.dirname(__file__), "data", "player_profile_pit.json"), "rt") as fp:
            srcdata.extend(json.load(fp))
        rmgr = self._getMockPlayerProfileManager(rosterData=rstdata, profileData=srcdata)
        recs = util.runCoroutine(rmgr.sync())
        self.assertEqual(len(recs), len(srcdata), "sync returned record count differs")
        dbrecs = util.runCoroutine(self.entmgr.find(self.entityName))
        self.assertEqual(dbrecs, recs, "db records differ")
        self.assertEqual(rmgr._apiClient.getRequestedRosters(), rstdata, "requested rosters differs")

    def test_sync_only_updates_records_with_team_change(self):
        with open(os.path.join(os.path.dirname(__file__), "data", "roster_kc.json"), "rt") as fp:
            rstdata = json.load(fp)
        with open(os.path.join(os.path.dirname(__file__), "data", "roster_pit.json"), "rt") as fp:
            rstdata.extend(json.load(fp))
        with open(os.path.join(os.path.dirname(__file__), "data", "player_profile_kc.json"), "rt") as fp:
            srcdata = json.load(fp)
        with open(os.path.join(os.path.dirname(__file__), "data", "player_profile_pit.json"), "rt") as fp:
            srcdata.extend(json.load(fp))
        rmgr = self._getMockPlayerProfileManager(rosterData=rstdata, profileData=srcdata)
        recs = util.runCoroutine(rmgr.sync())
        xreqrec = []
        for rec in rstdata:
            if rec["profile_id"] == 2560950:
                rec["team"] = "KC"
                xreqrec.append(rec)
        xrec = []
        for rec in srcdata:
            if rec["profile_id"] == 2560950:
                rec["team"] = "KC"
                crec = rec.copy()
                # Since the team is changing the previous_teams attribute should be set
                crec["previous_teams"] = ["PIT"]
                xrec.append(crec)
        rmgr = self._getMockPlayerProfileManager(rosterData=rstdata, profileData=srcdata)
        recs = util.runCoroutine(rmgr.sync())
        self.assertEqual(rmgr._apiClient.getRequestedRosters(), xreqrec, "requested rosters differs")
        self.assertEqual(len(recs), 1, "sync returned record count differs")
        dbrecs = util.runCoroutine(self.entmgr.find(self.entityName,
                                                    query={"profile_id": 2560950},
                                                    projection={"_id": False}))
        self.assertEqual(len(dbrecs), 1, "db record counts differ")
        self.assertEqual(dbrecs, xrec, "db records differ")

    def test_sync_updates_nothing_with_no_team_change(self):
        with open(os.path.join(os.path.dirname(__file__), "data", "roster_kc.json"), "rt") as fp:
            rstdata = json.load(fp)
        with open(os.path.join(os.path.dirname(__file__), "data", "roster_pit.json"), "rt") as fp:
            rstdata.extend(json.load(fp))
        with open(os.path.join(os.path.dirname(__file__), "data", "player_profile_kc.json"), "rt") as fp:
            srcdata = json.load(fp)
        with open(os.path.join(os.path.dirname(__file__), "data", "player_profile_pit.json"), "rt") as fp:
            srcdata.extend(json.load(fp))
        rmgr = self._getMockPlayerProfileManager(rosterData=rstdata, profileData=srcdata)
        recs = util.runCoroutine(rmgr.sync())
        rmgr = self._getMockPlayerProfileManager(rosterData=rstdata, profileData=srcdata)
        recs = util.runCoroutine(rmgr.sync())
        self.assertEqual(len(recs), 0, "sync returned record count differs")
        self.assertEqual(rmgr._apiClient.getRequestedRosters(), [], "requested rosters differs")

    def test_sync_updates_all_with_all(self):
        with open(os.path.join(os.path.dirname(__file__), "data", "roster_kc.json"), "rt") as fp:
            rstdata = json.load(fp)
        with open(os.path.join(os.path.dirname(__file__), "data", "roster_pit.json"), "rt") as fp:
            rstdata.extend(json.load(fp))
        with open(os.path.join(os.path.dirname(__file__), "data", "player_profile_kc.json"), "rt") as fp:
            srcdata = json.load(fp)
        with open(os.path.join(os.path.dirname(__file__), "data", "player_profile_pit.json"), "rt") as fp:
            srcdata.extend(json.load(fp))
        rmgr = self._getMockPlayerProfileManager(rosterData=rstdata, profileData=srcdata)
        recs = util.runCoroutine(rmgr.sync())
        def pidmap(recs : List[dict]) -> dict:
            return dict(zip([_["profile_id"] for _ in recs], recs))
        rmap = pidmap(rstdata.copy())
        pmap = pidmap(srcdata.copy())
        usrcdata = []
        for pid in rmap:
            t = random.choice(["KC", "PIT"])
            rmap[pid]["team"] = t
            pmap[pid]["team"] = t
            usrcdata.append(pmap[pid])
        rmgr = self._getMockPlayerProfileManager(rosterData=rmap.values(), profileData=usrcdata)
        recs = util.runCoroutine(rmgr.sync(all=True))
        self.assertEqual(len(recs), len(usrcdata), "sync returned record count differs")
        dbrecs = util.runCoroutine(self.entmgr.find(self.entityName, projection={"_id": False}))
        self.assertEqual(dbrecs, usrcdata, "db records differ")

    def test_save_appends(self):
        with open(os.path.join(os.path.dirname(__file__), "data", "roster_kc.json"), "rt") as fp:
            kcrdata = json.load(fp)
        with open(os.path.join(os.path.dirname(__file__), "data", "player_profile_kc.json"), "rt") as fp:
            kcdata = json.load(fp)
        with open(os.path.join(os.path.dirname(__file__), "data", "player_profile_pit.json"), "rt") as fp:
            pitdata = json.load(fp)
        rmgr = self._getMockPlayerProfileManager(rosterData=kcrdata, profileData=kcdata.copy())
        recs = util.runCoroutine(rmgr.sync())
        self.assertEqual(len(recs), len(kcdata), "sync record count differs")
        recs.extend(util.runCoroutine(rmgr.save(pitdata.copy())))
        self.assertEqual(len(recs), len(kcdata) + len(pitdata), "save record count differs")
        dbrecs = util.runCoroutine(self.entmgr.find(self.entityName))
        self.assertEqual(len(dbrecs), len(recs), "db record count differs")
        self.assertEqual(dbrecs, recs, "db records differ")

    def test_save_updates_previous_team(self):
        with open(os.path.join(os.path.dirname(__file__), "data", "roster_kc.json"), "rt") as fp:
            srcrdata = json.load(fp)
        with open(os.path.join(os.path.dirname(__file__), "data", "roster_pit.json"), "rt") as fp:
            srcrdata.extend(json.load(fp))
        with open(os.path.join(os.path.dirname(__file__), "data", "player_profile_kc.json"), "rt") as fp:
            kcdata = json.load(fp)
        with open(os.path.join(os.path.dirname(__file__), "data", "player_profile_pit.json"), "rt") as fp:
            pitdata = json.load(fp)
        srcdata = kcdata.copy()
        srcdata.extend(pitdata.copy())
        rmgr = self._getMockPlayerProfileManager(rosterData=srcrdata, profileData=srcdata)
        recs = util.runCoroutine(rmgr.sync())
        self.assertEqual(len(recs), len(srcdata), "sync record count differs")
        for rec in pitdata:
            if rec["profile_id"] == 2560950:
                rec["team"] = "KC"
                kcdata.append(rec)
        recs2 = util.runCoroutine(rmgr.save(kcdata.copy()))
        self.assertEqual(len(recs2), len(kcdata), "save record count differs")
        dbrecs = util.runCoroutine(self.entmgr.find(self.entityName, projection={"_id": False}))
        self.assertEqual(len(dbrecs), len(srcdata), "db record count differs")
        for rec in srcdata:
            if rec["profile_id"] == 2560950:
                rec["team"] = "KC"
                rec["previous_teams"] = ["PIT"]
        self.assertEqual(dbrecs, srcdata, "db records differ")

    def test_delete_team(self):
        with open(os.path.join(os.path.dirname(__file__), "data", "player_profile_kc.json"), "rt") as fp:
            kcdata = json.load(fp)
        with open(os.path.join(os.path.dirname(__file__), "data", "player_profile_pit.json"), "rt") as fp:
            pitdata = json.load(fp)
        srcdata = kcdata.copy()
        srcdata.extend(pitdata.copy())
        rmgr = self._getPlayerProfileManager()
        recs = util.runCoroutine(rmgr.save(srcdata))
        self.assertEqual(len(recs), len(srcdata), "save returned record count differs")
        dcount = util.runCoroutine(rmgr.delete(teams=["PIT"]))
        self.assertEqual(dcount, len(pitdata), "delete returned record count differs")
        dbrecs = util.runCoroutine(self.entmgr.find(self.entityName))
        self.assertEqual(len(dbrecs), len(kcdata), "db record count differs")
        for rec in dbrecs:
            del rec["_id"]
        self.assertEqual(dbrecs, kcdata, "db records differ")

    def test_delete_profile_id(self):
        with open(os.path.join(os.path.dirname(__file__), "data", "player_profile_kc.json"), "rt") as fp:
            srcdata = json.load(fp)
        rmgr = self._getPlayerProfileManager()
        recs = util.runCoroutine(rmgr.save(srcdata))
        self.assertEqual(len(recs), len(srcdata), "save returned record count differs")
        dcount = util.runCoroutine(rmgr.delete(profile_ids=[2562399]))
        self.assertEqual(dcount, 1, "delete returned record count differs")
        dbrecs = util.runCoroutine(self.entmgr.find(self.entityName))
        self.assertEqual(len(dbrecs), len(srcdata) - 1, "db record count differs")
        xdata = [_ for _ in srcdata if _["profile_id"] != 2562399]
        self.assertEqual(dbrecs, xdata, "db records differ")

class MockRosterManagerFacade(RosterManagerFacade):
    def __init__(self, entityManager : EntityManager, apiClient : nflapi.Client.Client, findData : List[dict]):
        super(MockRosterManagerFacade, self).__init__(entityManager, apiClient)
        self._find_data = findData

    async def find(self, *args) -> List[dict]:
        return self._find_data

class MockApiClient(nflapi.Client.Client):
    def __init__(self, profileData : List[dict]):
        self._profile_data = dict(zip([_["profile_id"] for _ in profileData], profileData))
        self._req_rosters = []

    def getPlayerProfile(self, rosters : List[str]) -> List[dict]:
        self._req_rosters = rosters
        data = []
        for pid in [_["profile_id"] for _ in rosters]:
            if pid in self._profile_data:
                data.append(self._profile_data[pid])
        return data

    def getRequestedRosters(self) -> List[dict]:
        return self._req_rosters
