import unittest
import os
import json
from typing import List
import nflapi.Client
from nflapidb.TeamManagerFacade import TeamManagerFacade
from nflapidb.RosterManagerFacade import RosterManagerFacade
from nflapidb.EntityManager import EntityManager
import nflapidb.Utilities as util

class TestRosterManagerFacade(unittest.TestCase):

    def setUp(self):
        self.entityName = "roster"
        self.entmgr = EntityManager()

    def tearDown(self):
        util.runCoroutine(self.entmgr.drop(self.entityName))
        self.entmgr.dispose()

    def _getMockRosterManager(self, teamData : List[dict], rosterData : List[dict]):
        apiClient = MockApiClient(rosterData)
        tmmgr = MockTeamManagerFacade(self.entmgr, apiClient, teamData)
        self.datamgr = RosterManagerFacade(self.entmgr, apiClient, tmmgr)
        return self.datamgr

    def _getRosterManager(self):
        self.datamgr = RosterManagerFacade(self.entmgr)
        return self.datamgr

    def test_sync_initializes_collection_one_team(self):
        with open(os.path.join(os.path.dirname(__file__), "data", "roster_kc.json"), "rt") as fp:
            srcdata = json.load(fp)
        rmgr = self._getMockRosterManager(teamData=[{"team": "KC"}], rosterData=srcdata)
        recs = util.runCoroutine(rmgr.sync())
        self.assertEqual(len(recs), len(srcdata), "sync returned record count differs")
        dbrecs = util.runCoroutine(self.entmgr.find(self.entityName))
        self.assertEqual(dbrecs, recs, "db records differ")
        self.assertEqual(rmgr._apiClient.getRequestedTeams(), set(["KC"]), "requested teams differs")

    def test_sync_initializes_collection_two_teams(self):
        with open(os.path.join(os.path.dirname(__file__), "data", "roster_kc.json"), "rt") as fp:
            srcdata = json.load(fp)
        with open(os.path.join(os.path.dirname(__file__), "data", "roster_pit.json"), "rt") as fp:
            srcdata.extend(json.load(fp))
        rmgr = self._getMockRosterManager(teamData=[{"team": "KC"}, {"team": "PIT"}], rosterData=srcdata)
        recs = util.runCoroutine(rmgr.sync())
        self.assertEqual(len(recs), len(srcdata), "sync returned record count differs")
        dbrecs = util.runCoroutine(self.entmgr.find(self.entityName))
        self.assertEqual(dbrecs, recs, "db records differ")
        self.assertEqual(rmgr._apiClient.getRequestedTeams(), set(["KC", "PIT"]), "requested teams differs")

    def test_sync_stores_previous_team(self):
        teamData = [{"team": "KC"}, {"team": "PIT"}]
        with open(os.path.join(os.path.dirname(__file__), "data", "roster_kc.json"), "rt") as fp:
            srcdata = json.load(fp)
        with open(os.path.join(os.path.dirname(__file__), "data", "roster_pit.json"), "rt") as fp:
            srcdata.extend(json.load(fp))
        rmgr = self._getMockRosterManager(teamData=teamData, rosterData=srcdata)
        recs = util.runCoroutine(rmgr.sync())
        for rec in srcdata:
            if rec["profile_id"] == 2560950:
                rec["team"] = "KC"
        rmgr = self._getMockRosterManager(teamData=teamData, rosterData=srcdata)
        recs = util.runCoroutine(rmgr.sync())
        self.assertEqual(len(recs), len(srcdata), "sync returned record count differs")
        dbrecs = util.runCoroutine(self.entmgr.find(self.entityName))
        self.assertEqual(dbrecs, recs, "db records differ")
        self.assertEqual(rmgr._apiClient.getRequestedTeams(), set(["KC", "PIT"]), "requested teams differs")
        xurecs = [_ for _ in dbrecs if _["profile_id"] == 2560950]
        self.assertEqual(len(xurecs), 1, "updated record count differs")
        self.assertTrue("previous_teams" in xurecs[0], "previous_teams attribute missing")
        self.assertEqual(xurecs[0]["previous_teams"], ["PIT"], "previous_teams value differs")

    def test_save_appends(self):
        with open(os.path.join(os.path.dirname(__file__), "data", "roster_kc.json"), "rt") as fp:
            kcdata = json.load(fp)
        with open(os.path.join(os.path.dirname(__file__), "data", "roster_pit.json"), "rt") as fp:
            pitdata = json.load(fp)
        rmgr = self._getMockRosterManager(teamData=[{"team": "KC"}], rosterData=kcdata.copy())
        recs = util.runCoroutine(rmgr.sync())
        self.assertEqual(len(recs), len(kcdata), "sync record count differs")
        recs.extend(util.runCoroutine(rmgr.save(pitdata.copy())))
        self.assertEqual(len(recs), len(kcdata) + len(pitdata), "save record count differs")
        dbrecs = util.runCoroutine(self.entmgr.find(self.entityName))
        self.assertEqual(len(dbrecs), len(recs), "db record count differs")
        self.assertEqual(dbrecs, recs, "db records differ")

    def test_save_updates_previous_team(self):
        with open(os.path.join(os.path.dirname(__file__), "data", "roster_kc.json"), "rt") as fp:
            kcdata = json.load(fp)
        with open(os.path.join(os.path.dirname(__file__), "data", "roster_pit.json"), "rt") as fp:
            pitdata = json.load(fp)
        srcdata = kcdata.copy()
        srcdata.extend(pitdata.copy())
        rmgr = self._getMockRosterManager(teamData=[{"team": "KC"}, {"team": "PIT"}], rosterData=srcdata)
        recs = util.runCoroutine(rmgr.sync())
        self.assertEqual(len(recs), len(srcdata), "sync record count differs")
        for rec in pitdata:
            if rec["profile_id"] == 2560950:
                rec["team"] = "KC"
                kcdata.append(rec)
        recs2 = util.runCoroutine(rmgr.save(kcdata.copy()))
        self.assertEqual(len(recs2), len(kcdata), "save record count differs")
        dbrecs = util.runCoroutine(self.entmgr.find(self.entityName))
        self.assertEqual(len(dbrecs), len(srcdata), "db record count differs")
        for rec in srcdata:
            if rec["profile_id"] == 2560950:
                rec["team"] = "KC"
                rec["previous_teams"] = ["PIT"]
        self.assertEqual(dbrecs, srcdata, "db records differ")

    def test_delete_team(self):
        with open(os.path.join(os.path.dirname(__file__), "data", "roster_kc.json"), "rt") as fp:
            kcdata = json.load(fp)
        with open(os.path.join(os.path.dirname(__file__), "data", "roster_pit.json"), "rt") as fp:
            pitdata = json.load(fp)
        srcdata = kcdata.copy()
        srcdata.extend(pitdata.copy())
        rmgr = self._getRosterManager()
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
        with open(os.path.join(os.path.dirname(__file__), "data", "roster_kc.json"), "rt") as fp:
            srcdata = json.load(fp)
        rmgr = self._getRosterManager()
        recs = util.runCoroutine(rmgr.save(srcdata))
        self.assertEqual(len(recs), len(srcdata), "save returned record count differs")
        dcount = util.runCoroutine(rmgr.delete(profile_ids=[2562399]))
        self.assertEqual(dcount, 1, "delete returned record count differs")
        dbrecs = util.runCoroutine(self.entmgr.find(self.entityName))
        self.assertEqual(len(dbrecs), len(srcdata) - 1, "db record count differs")
        xdata = [_ for _ in srcdata if _["profile_id"] != 2562399]
        self.assertEqual(dbrecs, xdata, "db records differ")

    def test__getQueryModel_player_abbreviations_one(self):
        rmgr = self._getRosterManager()
        qm = rmgr._getQueryModel(player_abbreviations=["C.Wollam"])
        xconst = {"$and": [
            {"first_name": {"$regex": "^c.*", "$options": "i"}},
            {"last_name": {"$regex": "^wollam", "$options": "i"}}
        ]}
        self.assertEqual(qm.constraint, xconst)

    def test__getQueryModel_player_abbreviations_one_suffix(self):
        rmgr = self._getRosterManager()
        qm = rmgr._getQueryModel(player_abbreviations=["C.Wollam Jr."])
        xconst = {"$and": [
            {"first_name": {"$regex": "^c.*", "$options": "i"}},
            {"last_name": {"$regex": "^wollam( *jr)*", "$options": "i"}}
        ]}
        self.assertEqual(qm.constraint, xconst)

    def test__getQueryModel_player_abbreviations_two(self):
        rmgr = self._getRosterManager()
        qm = rmgr._getQueryModel(player_abbreviations=["C.Wollam", "M.King"])
        xconst = {"$or": [
            {"$and": [
                {"first_name": {"$regex": "^c.*", "$options": "i"}},
                {"last_name": {"$regex": "^wollam", "$options": "i"}}
            ]},
            {"$and": [
                {"first_name": {"$regex": "^m.*", "$options": "i"}},
                {"last_name": {"$regex": "^king", "$options": "i"}}
            ]}
        ]}
        self.assertEqual(qm.constraint, xconst)

class MockTeamManagerFacade(TeamManagerFacade):
    def __init__(self, entityManager : EntityManager, apiClient : nflapi.Client.Client, findData : List[dict]):
        super(MockTeamManagerFacade, self).__init__(entityManager, apiClient)
        self._find_data = findData

    async def find(self, *args) -> List[dict]:
        return self._find_data

class MockApiClient(nflapi.Client.Client):
    def __init__(self, rosterData : List[dict]):
        self._roster_data = rosterData
        self._req_teams = set()

    def getRoster(self, teams : List[str]) -> List[dict]:
        self._req_teams.update(teams)
        return self._roster_data

    def getRequestedTeams(self) -> set:
        return self._req_teams
