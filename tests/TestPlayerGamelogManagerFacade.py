import unittest
import unittest.mock
import os
import json
from typing import List, Set
import random
import datetime
import nflapi.Client
from nflapidb.RosterManagerFacade import RosterManagerFacade
from nflapidb.PlayerGamelogManagerFacade import PlayerGamelogManagerFacade
from nflapidb.EntityManager import EntityManager
import nflapidb.Utilities as util

class TestPlayerGamelogManagerFacade(unittest.TestCase):

    def setUp(self):
        self.entityName = "player_gamelog"
        self.entmgr = EntityManager()

    def tearDown(self):
        util.runCoroutine(self.entmgr.drop(self.entityName))
        util.runCoroutine(self.entmgr.drop(f"{self.entityName}_process"))
        self.entmgr.dispose()

    def _getMockPlayerGamelogManager(self, rosterData : List[dict], gamelogData : List[dict], dataExpired : bool = None):
        apiClient = MockApiClient(gamelogData)
        rmgr = MockRosterManagerFacade(self.entmgr, apiClient, rosterData)
        self.datamgr = MockPlayerGamelogManagerFacade(self.entmgr, apiClient, rmgr, dataExpired)
        return self.datamgr

    def _getPlayerGamelogManager(self):
        self.datamgr = PlayerGamelogManagerFacade(self.entmgr)
        return self.datamgr

    def _compareGL(self, testrecs : List[dict], exprecs : List[dict]):
        testrecs = __sortgl__(testrecs)
        exprecs = __sortgl__(__getUniqueGL__(exprecs))
        self.assertEqual(len(testrecs), len(exprecs), "record counts differs")
        for i in range(0, len(testrecs)):
            try:
                self.assertEqual(testrecs[i], exprecs[i], f"record {i} differs")
            except AssertionError as e:
                raise e

    def test_sync_initializes_collection_one_team(self):
        with open(os.path.join(os.path.dirname(__file__), "data", "roster_kc.json"), "rt") as fp:
            rstdata = json.load(fp)
        with open(os.path.join(os.path.dirname(__file__), "data", "player_gamelog_kc.json"), "rt") as fp:
            srcdata = json.load(fp)
        rmgr = self._getMockPlayerGamelogManager(rosterData=rstdata, gamelogData=srcdata)
        recs = util.runCoroutine(rmgr.sync())
        self.assertEqual(len(recs), len(srcdata), "sync returned record count differs")
        dbrecs = util.runCoroutine(self.entmgr.find(self.entityName))
        self._compareGL(dbrecs, recs)
        self.assertEqual(rmgr._apiClient.getRequestedRosters(), rstdata, "requested rosters differs")

    def test_sync_initializes_collection_two_teams(self):
        with open(os.path.join(os.path.dirname(__file__), "data", "roster_kc.json"), "rt") as fp:
            rstdata = json.load(fp)
        with open(os.path.join(os.path.dirname(__file__), "data", "roster_pit.json"), "rt") as fp:
            rstdata.extend(json.load(fp))
        with open(os.path.join(os.path.dirname(__file__), "data", "player_gamelog_kc.json"), "rt") as fp:
            srcdata = json.load(fp)
        with open(os.path.join(os.path.dirname(__file__), "data", "player_gamelog_pit.json"), "rt") as fp:
            srcdata.extend(json.load(fp))
        rmgr = self._getMockPlayerGamelogManager(rosterData=rstdata, gamelogData=srcdata)
        recs = util.runCoroutine(rmgr.sync())
        self.assertEqual(len(recs), len(srcdata), "sync returned record count differs")
        dbrecs = util.runCoroutine(self.entmgr.find(self.entityName))
        self._compareGL(dbrecs, recs)
        self.assertEqual(rmgr._apiClient.getRequestedRosters(), rstdata, "requested rosters differs")

    def test_sync_only_updates_records_with_team_change(self):
        with open(os.path.join(os.path.dirname(__file__), "data", "roster_kc.json"), "rt") as fp:
            rstdata = json.load(fp)
        with open(os.path.join(os.path.dirname(__file__), "data", "roster_pit.json"), "rt") as fp:
            rstdata.extend(json.load(fp))
        with open(os.path.join(os.path.dirname(__file__), "data", "player_gamelog_kc.json"), "rt") as fp:
            srcdata = json.load(fp)
        with open(os.path.join(os.path.dirname(__file__), "data", "player_gamelog_pit.json"), "rt") as fp:
            srcdata.extend(json.load(fp))
        rmgr = self._getMockPlayerGamelogManager(rosterData=rstdata, gamelogData=srcdata)
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
        rmgr = self._getMockPlayerGamelogManager(rosterData=rstdata, gamelogData=srcdata, dataExpired=False)
        recs = util.runCoroutine(rmgr.sync())
        reqrec = rmgr._apiClient.getRequestedRosters()
        self.assertEqual(len(reqrec), len(xreqrec), "requested roster lengths differs")
        self.assertEqual(reqrec, xreqrec, "requested rosters differs")
        self.assertEqual(len(recs), 24, "sync returned record count differs")
        dbrecs = util.runCoroutine(self.entmgr.find(self.entityName,
                                                    query={"profile_id": 2560950},
                                                    projection={"_id": False}))
        self.assertEqual(len(dbrecs), 24, "db record counts differ")
        self._compareGL(dbrecs, xrec)

    def test_sync_updates_nothing_with_no_team_change(self):
        with open(os.path.join(os.path.dirname(__file__), "data", "roster_kc.json"), "rt") as fp:
            rstdata = json.load(fp)
        with open(os.path.join(os.path.dirname(__file__), "data", "roster_pit.json"), "rt") as fp:
            rstdata.extend(json.load(fp))
        with open(os.path.join(os.path.dirname(__file__), "data", "player_gamelog_kc.json"), "rt") as fp:
            srcdata = json.load(fp)
        with open(os.path.join(os.path.dirname(__file__), "data", "player_gamelog_pit.json"), "rt") as fp:
            srcdata.extend(json.load(fp))
        rmgr = self._getMockPlayerGamelogManager(rosterData=rstdata, gamelogData=srcdata)
        recs = util.runCoroutine(rmgr.sync())
        rmgr = self._getMockPlayerGamelogManager(rosterData=rstdata, gamelogData=srcdata, dataExpired=False)
        recs = util.runCoroutine(rmgr.sync())
        self.assertEqual(len(recs), 0, "sync returned record count differs")
        self.assertEqual(rmgr._apiClient.getRequestedRosters(), [], "requested rosters differs")

    def test_sync_updates_all_with_all(self):
        with open(os.path.join(os.path.dirname(__file__), "data", "roster_kc.json"), "rt") as fp:
            rstdata = json.load(fp)
        with open(os.path.join(os.path.dirname(__file__), "data", "roster_pit.json"), "rt") as fp:
            rstdata.extend(json.load(fp))
        with open(os.path.join(os.path.dirname(__file__), "data", "player_gamelog_kc.json"), "rt") as fp:
            srcdata = json.load(fp)
        with open(os.path.join(os.path.dirname(__file__), "data", "player_gamelog_pit.json"), "rt") as fp:
            srcdata.extend(json.load(fp))
        rmgr = self._getMockPlayerGamelogManager(rosterData=rstdata, gamelogData=srcdata)
        util.runCoroutine(rmgr.sync())
        def pidmap(recs : List[dict]) -> dict:
            return dict(zip([_["profile_id"] for _ in recs], recs))
        rmap = pidmap(rstdata.copy())
        usrcdata = []
        for rec in srcdata:
            pid = rec["profile_id"]
            t = random.choice(["KC", "PIT"])
            rmap[pid]["team"] = t
            rec["team"] = t
            usrcdata.append(rec)
        rmgr = self._getMockPlayerGamelogManager(rosterData=rmap.values(), gamelogData=usrcdata)
        util.runCoroutine(rmgr.sync(all=True))
        dbrecs = util.runCoroutine(self.entmgr.find(self.entityName, projection={"_id": False}))
        self._compareGL(dbrecs, usrcdata)

    def test_sync_updates_current_season_only_with_week_diff(self):
        with open(os.path.join(os.path.dirname(__file__), "data", "roster_kc.json"), "rt") as fp:
            rstdata = json.load(fp)
        with open(os.path.join(os.path.dirname(__file__), "data", "roster_pit.json"), "rt") as fp:
            rstdata.extend(json.load(fp))
        with open(os.path.join(os.path.dirname(__file__), "data", "player_gamelog_kc.json"), "rt") as fp:
            alldata = json.load(fp)
        with open(os.path.join(os.path.dirname(__file__), "data", "player_gamelog_pit.json"), "rt") as fp:
            alldata.extend(json.load(fp))
        def insmaxgame(d : dict, rec : dict, path : List[str] = ["season", "season_type", "wk"]):
            for k in path:
                if len(d) > 0:
                    dv = list(d.keys())[0]
                    del d[dv]
                v = rec[k]
                d[v] = {}
                d = d[v]
        def maxgame(recs : dict) -> dict:
            st = ["preseason", "regular_season", "postseason"]
            md = {}
            for rec in recs:
                d = md
                if len(d) == 0:
                    insmaxgame(d, rec)
                else:
                    v = rec["season"]
                    cv = list(d.keys())[0]
                    if v > cv:
                        insmaxgame(d, rec)
                    elif v == cv:
                        d = d[cv]
                        v = rec["season_type"]
                        cv = list(d.keys())[0]
                        if st.index(v) > st.index(cv):
                            insmaxgame(d, rec, ["season_type", "wk"])
                        elif st.index(v) == st.index(cv):
                            d = d[cv]
                            v = rec["wk"]
                            cv = list(d.keys())[0]
                            if v > cv:
                                insmaxgame(d, rec, ["wk"])
            return md
        def ismaxgame(rec : dict, mgd : dict) -> bool:
            b = False
            d = mgd
            for k in ["season", "season_type", "wk"]:
                v = rec[k]
                if v in d:
                    d = d[v]
                    if len(d) == 0:
                        b = True
                else:
                    break
            return b
        mg = maxgame(alldata)  
        srcdata1 = []
        srcdata2 = []
        for rec in alldata:
            if not ismaxgame(rec, mg):
                srcdata1.append(rec)
            srcdata2.append(rec)
        rmgr = self._getMockPlayerGamelogManager(rosterData=rstdata, gamelogData=srcdata1)
        util.runCoroutine(rmgr.sync())
        rmgr = self._getMockPlayerGamelogManager(rosterData=rstdata, gamelogData=srcdata2)
        util.runCoroutine(rmgr._updateDataExpired(datetime.datetime.today() - datetime.timedelta(weeks=1)))
        util.runCoroutine(rmgr.sync())
        dbrecs = util.runCoroutine(self.entmgr.find(self.entityName, projection={"_id": False}))
        self._compareGL(dbrecs, srcdata2)
        self.assertEqual(rmgr._apiClient.getRequestedSeasons(), set([rmgr._currentSeason]), "requested seasons differs")

    def test_save_appends(self):
        with open(os.path.join(os.path.dirname(__file__), "data", "roster_kc.json"), "rt") as fp:
            kcrdata = json.load(fp)
        with open(os.path.join(os.path.dirname(__file__), "data", "player_gamelog_kc.json"), "rt") as fp:
            kcdata = json.load(fp)
        with open(os.path.join(os.path.dirname(__file__), "data", "player_gamelog_pit.json"), "rt") as fp:
            pitdata = json.load(fp)
        rmgr = self._getMockPlayerGamelogManager(rosterData=kcrdata, gamelogData=kcdata.copy())
        recs = util.runCoroutine(rmgr.sync())
        self.assertEqual(len(recs), len(kcdata), "sync record count differs")
        recs.extend(util.runCoroutine(rmgr.save(pitdata.copy())))
        self.assertEqual(len(recs), len(kcdata) + len(pitdata), "save record count differs")
        dbrecs = util.runCoroutine(self.entmgr.find(self.entityName))
        self._compareGL(dbrecs, recs)

    def test_save_updates_previous_team(self):
        with open(os.path.join(os.path.dirname(__file__), "data", "roster_kc.json"), "rt") as fp:
            srcrdata = json.load(fp)
        with open(os.path.join(os.path.dirname(__file__), "data", "roster_pit.json"), "rt") as fp:
            srcrdata.extend(json.load(fp))
        with open(os.path.join(os.path.dirname(__file__), "data", "player_gamelog_kc.json"), "rt") as fp:
            kcdata = json.load(fp)
        with open(os.path.join(os.path.dirname(__file__), "data", "player_gamelog_pit.json"), "rt") as fp:
            pitdata = json.load(fp)
        srcdata = kcdata.copy()
        srcdata.extend(pitdata.copy())
        rmgr = self._getMockPlayerGamelogManager(rosterData=srcrdata, gamelogData=srcdata)
        recs = util.runCoroutine(rmgr.sync())
        self.assertEqual(len(recs), len(srcdata), "sync record count differs")
        for rec in pitdata:
            if rec["profile_id"] == 2560950:
                rec["team"] = "KC"
                kcdata.append(rec)
        recs2 = util.runCoroutine(rmgr.save(kcdata.copy()))
        self.assertEqual(len(recs2), len(kcdata), "save record count differs")
        dbrecs = util.runCoroutine(self.entmgr.find(self.entityName, projection={"_id": False}))
        for rec in srcdata:
            if rec["profile_id"] == 2560950:
                rec["team"] = "KC"
                rec["previous_teams"] = ["PIT"]
        self._compareGL(dbrecs, srcdata)

    def test_delete_team(self):
        with open(os.path.join(os.path.dirname(__file__), "data", "player_gamelog_kc.json"), "rt") as fp:
            kcdata = json.load(fp)
        with open(os.path.join(os.path.dirname(__file__), "data", "player_gamelog_pit.json"), "rt") as fp:
            pitdata = json.load(fp)
        srcdata = kcdata.copy()
        srcdata.extend(pitdata.copy())
        rmgr = self._getPlayerGamelogManager()
        recs = util.runCoroutine(rmgr.save(srcdata))
        self.assertEqual(len(recs), len(srcdata), "save returned record count differs")
        dcount = util.runCoroutine(rmgr.delete(teams=["PIT"]))
        self.assertEqual(dcount, len(pitdata), "delete returned record count differs")
        dbrecs = util.runCoroutine(self.entmgr.find(self.entityName))
        for rec in dbrecs:
            del rec["_id"]
        self._compareGL(dbrecs, kcdata)

    def test_delete_profile_id(self):
        with open(os.path.join(os.path.dirname(__file__), "data", "player_gamelog_kc.json"), "rt") as fp:
            srcdata = json.load(fp)
        xdelrecs = [_ for _ in srcdata if _["profile_id"] == 2562399]
        xdata = [_ for _ in srcdata if _["profile_id"] != 2562399]
        rmgr = self._getPlayerGamelogManager()
        recs = util.runCoroutine(rmgr.save(srcdata))
        self.assertEqual(len(recs), len(srcdata), "save returned record count differs")
        dcount = util.runCoroutine(rmgr.delete(profile_ids=[2562399]))
        self.assertEqual(dcount, len(xdelrecs), "delete returned record count differs")
        dbrecs = util.runCoroutine(self.entmgr.find(self.entityName))
        for rec in dbrecs:
            del rec["_id"]
        self._compareGL(dbrecs, xdata)

class MockPlayerGamelogManagerFacade(PlayerGamelogManagerFacade):
    def __init__(self, entityManager : EntityManager,
                 apiClient : nflapi.Client.Client = None,
                 rosterManager : RosterManagerFacade = None,
                 dataExpired : bool = None):
        super(MockPlayerGamelogManagerFacade, self).__init__(entityManager, apiClient, rosterManager)
        self._data_expired = dataExpired

    async def _isDataExpired(self) -> bool:
        if self._data_expired is not None:
            return self._data_expired
        else:
            return await super(MockPlayerGamelogManagerFacade, self)._isDataExpired()

class MockRosterManagerFacade(RosterManagerFacade):
    def __init__(self, entityManager : EntityManager, apiClient : nflapi.Client.Client, findData : List[dict]):
        super(MockRosterManagerFacade, self).__init__(entityManager, apiClient)
        self._find_data = findData

    async def find(self, *args) -> List[dict]:
        return self._find_data

class MockApiClient(nflapi.Client.Client):
    def __init__(self, gamelogData : List[dict]):
        self._gamelog_data = {}
        for datum in gamelogData:
            pid = datum["profile_id"]
            ssn = datum["season"]
            if pid in self._gamelog_data:
                d = self._gamelog_data[pid]
                if ssn in d:
                    d[ssn].append(datum)
                else:
                    d[ssn] = [datum]
            else:
                self._gamelog_data[pid] = dict([(ssn, [datum])])
        self._req_rosters = []
        self._req_seasons = set()

    def getPlayerGameLog(self, rosters : List[str], season : int) -> List[dict]:
        self._req_rosters = rosters
        self._req_seasons.add(season)
        data = []
        for rec in rosters:
            pid = rec["profile_id"]
            if pid in self._gamelog_data and season in self._gamelog_data[pid]:
                data.extend(self._gamelog_data[pid][season])
        return data

    def getRequestedRosters(self) -> List[dict]:
        return self._req_rosters

    def getRequestedSeasons(self) -> Set[dict]:
        return self._req_seasons

def __getUniqueGL__(data : List[dict]) -> List[dict]:
    udata = []
    dup = {}
    i = 0
    for datum in data:
        ii = i
        pid = datum["profile_id"]
        if pid in dup:
            d = dup[pid]
            if datum["season"] in d:
                d = d[datum["season"]]
                if datum["season_type"] in d:
                    d = d[datum["season_type"]]
                    if datum["wk"] in d:
                        ii = d[datum["wk"]]
                    else:
                        d[datum["wk"]] = i
                else:
                    d[datum["season_type"]] = dict([(datum["wk"], i)])
            else:
                d[datum["season"]] = dict([(datum["season_type"], dict([(datum["wk"], i)]))])
        else:
            dup[pid] = dict([(datum["season"], dict([(datum["season_type"], dict([(datum["wk"], i)]))]))])
        if ii == i:
            udata.append(datum)
            i += 1
        else:
            udata[ii] = datum
    return udata

def __sortgl__(data : List[dict]) -> List[dict]:
    def f(datum : dict) -> str:
        return ",".join([str(datum["profile_id"]), str(datum["season"]), datum["season_type"], str(datum["wk"])])
    data.sort(key=f)
    return data