import unittest
import sys
import logging
import os
import json
import re
from datetime import date, datetime
from typing import List
from nflapidb.EntityManager import EntityManager
from nflapidb.TeamManagerFacade import TeamManagerFacade
from nflapidb.RosterManagerFacade import RosterManagerFacade
from nflapidb.ScheduleManagerFacade import ScheduleManagerFacade
from nflapidb.PlayerProfileManagerFacade import PlayerProfileManagerFacade
from nflapidb.PlayerGamelogManagerFacade import PlayerGamelogManagerFacade
from nflapidb.GameSummaryManagerFacade import GameSummaryManagerFacade
from nflapidb.GameScoreManagerFacade import GameScoreManagerFacade
from nflapidb.GameDriveManagerFacade import GameDriveManagerFacade
from nflapidb.GamePlayManagerFacade import GamePlayManagerFacade
import nflapidb.Utilities as util

class TestSync(unittest.TestCase):

    def setUp(self):
        logging.basicConfig(level=logging.INFO)
        self.entmgr = EntityManager(dbName="nflapidb_ut")

    def test_team(self):
        tmgr = TeamManagerFacade(self.entmgr)
        recs = util.runCoroutine(tmgr.sync())
        dbrecs = util.runCoroutine(tmgr.find())
        self.assertEqual(len(dbrecs), len(recs), "db records lengths differ")

    def test_roster(self):
        rmgr = RosterManagerFacade(self.entmgr)
        recs = util.runCoroutine(rmgr.sync())
        dbrecs = util.runCoroutine(rmgr.find())
        self.assertLessEqual(len(dbrecs), len(recs), "db records lengths differ")
        self.assertGreater(len([r for r in dbrecs if "previous_teams" in r]), 0, "previous_teams not set")

    def test_schedule(self):
        smgr = ScheduleManagerFacade(self.entmgr)
        recs = util.runCoroutine(smgr.sync())
        dbrecs = util.runCoroutine(smgr.find())
        self.assertEqual(len(dbrecs), len(recs), "db records lengths differ")

    def test_player_profile(self):
        pmgr = PlayerProfileManagerFacade(self.entmgr)
        recs = util.runCoroutine(pmgr.sync())
        dbrecs = util.runCoroutine(pmgr.find())
        self.assertEqual(len(dbrecs), len(recs), "db records lengths differ")
        self.assertGreater(len([r for r in dbrecs if "previous_teams" in r]), 0, "previous_teams not set")

    def test_player_gamelog(self):
        pmgr = PlayerGamelogManagerFacade(self.entmgr)
        recs = util.runCoroutine(pmgr.sync())
        dbrecs = util.runCoroutine(pmgr.find())
        self.assertEqual(len(dbrecs), len(recs), "db records lengths differ")
        self.assertGreater(len([r for r in dbrecs if "previous_teams" in r]), 0, "previous_teams not set")

    def test_game_summary(self):
        mgr = GameSummaryManagerFacade(self.entmgr)
        recs = util.runCoroutine(mgr.sync())
        dbrecs = util.runCoroutine(mgr.find())
        self.assertEqual(len(dbrecs), len(recs), "db records lengths differ")

    def test_game_score(self):
        mgr = GameScoreManagerFacade(self.entmgr)
        recs = util.runCoroutine(mgr.sync())
        dbrecs = util.runCoroutine(mgr.find())
        self.assertEqual(len(dbrecs), len(recs), "db records lengths differ")

    def test_game_drive(self):
        mgr = GameDriveManagerFacade(self.entmgr)
        recs = util.runCoroutine(mgr.sync())
        dbrecs = util.runCoroutine(mgr.find())
        self.assertEqual(len(dbrecs), len(recs), "db records lengths differ")

    def test_game_play(self):
        mgr = GamePlayManagerFacade(self.entmgr)
        recs = util.runCoroutine(mgr.sync())
        writeData(mgr._getAmbiguousPlayerAbbrevs(), "game_play_ambig_plabb.json")
        writeData(mgr._getMissingPlayerAbbrevs(), "game_play_miss_plabb.json")
        dbrecs = util.runCoroutine(mgr.find())
        self.assertEqual(len(dbrecs), len(recs), "db records lengths differ")

    @unittest.skip("manually run and review")
    def test_ambiguous_pl_abbrv(self):
        mgr = RosterManagerFacade(self.entmgr)
        mpabb = readData("game_play_ambig_plabb.json")
        qd = makeLastNameDict(mpabb, getLastNameFromAbbr)
        rdata = [d for d in util.runCoroutine(mgr.find(last_names=list(qd.keys()))) if d["position"] in ["QB", "RB", "WR", "TE", "K"]]
        rd = makeLastNameDict(rdata, lambda d: d["last_name"])
        hdata = readData("historic_roster.json", "data")
        hd = makeLastNameDict(hdata, lambda d: d["last_name"])
        updated = False
        for k in qd:
            if k in rd:
                for qr in qd[k]:
                    mrost = [r for r in rd[k] if matchAbbr(r, qr["player_abrv_name"])]
                    if len(mrost) == 1:
                        mhist = None
                        if k in hd:
                            mhista = [r for r in hd[k] if matchAbbr(r, qr["player_abrv_name"]) and r["position"] in ["QB", "RB", "WR", "TE", "K"]]
                            if len(mhista) == 1:
                                mhist = mhista[0]
                        if mhist is None:
                            mhist = mrost[0]
                            hdata.append(mhist)
                            hd[k].append(mhist)
                        pt = set([])
                        if "previous_teams" in mhist:
                            pt = set(mhist["previous_teams"])
                        pt.add(qr["team"])
                        ptl = list(pt)
                        ptl.sort()
                        mhist["previous_teams"] = ptl
                        updated = True
        if updated:
            writeData(convertDateToString(hdata), "historic_roster.json", "data")
        self.assertTrue(updated)

    def test_miss_pl_abbrv(self):
        mgr = RosterManagerFacade(self.entmgr)
        mpabb = readData("game_play_miss_plabb.json")
        qd = makeLastNameDict(mpabb, getLastNameFromAbbr)
        rdata = [d for d in util.runCoroutine(mgr.find(last_names=list(qd.keys()))) if d["position"] in ["QB", "RB", "WR", "TE", "K"]]
        rd = makeLastNameDict(rdata, lambda d: d["last_name"])
        hdata = readData("historic_roster.json", "data")
        hd = makeLastNameDict(hdata, lambda d: d["last_name"])
        updated = False
        for k in qd:
            if k in rd:
                for qr in qd[k]:
                    mrost = [r for r in rd[k] if matchAbbr(r, qr["player_abrv_name"])]
                    if len(mrost) == 1:
                        mhist = None
                        if k in hd:
                            # mhista = [r for r in hd[k] if matchAbbr(r, qr["player_abrv_name"]) and r["position"] in ["QB", "RB", "WR", "TE", "K"]]
                            mhista = []
                            for r in hd[k]:
                                if isinstance(r, list):
                                    mhista.extend(r)
                                else:
                                    mhista.append(r)
                            if len(mhista) == 1:
                                mhist = mhista[0]
                        if mhist is None:
                            mhist = mrost[0]
                            hdata.append(mhist)
                            if k in hd:
                                hd[k].append(mhist)
                            else:
                                hd[k] = [mhista]
                        pt = set([])
                        if "previous_teams" in mhist:
                            pt = set(mhist["previous_teams"])
                        pt.add(qr["team"])
                        ptl = list(pt)
                        ptl.sort()
                        mhist["previous_teams"] = ptl
                        updated = True
        if updated:
            writeData(convertDateToString(hdata), "historic_roster.json", "data")
        self.assertTrue(updated)

def getLastNameFromAbbr(d : dict = None, abbr : str = None) -> str:
    if abbr is None:
        abbr = d["player_abrv_name"]
    return re.sub(r"^[^\. ]+[\. ]", "", abbr)

def matchAbbr(d : dict, abbr : str) -> bool:
    fnre = "^{}".format(re.sub(r"\.", ".+", re.sub(r"^(.+[\. ]).+$", r"\1", re.sub(r"\.\.", ".", re.sub(" ", ".", abbr)))))
    lnre = getLastNameFromAbbr(abbr=abbr)
    return re.search(fnre, d["first_name"], re.IGNORECASE) is not None and re.search(lnre, d["last_name"], re.IGNORECASE)

def makeLastNameDict(rdata : List[dict], lnfun : callable) -> dict:
    lnd = {}
    for d in rdata:
        if not isinstance(d, dict):
            raise Exception("Data element not dict: {}".format(json.dumps(d, indent=4)))
        ln = lnfun(d)
        if ln not in lnd:
            lnd[ln] = [d]
        else:
            lnd[ln].append(d)
        if any([not isinstance(r, dict) for r in lnd[ln]]):
            print("Non-dict element detected: {}".format(json.dumps(lnd[ln])))
    return lnd

def convertDateToString(dl : List[dict], fields : list = ["birthdate"]) -> List[dict]:
    for d in dl:
        for f in fields:
            if isinstance(d[f], date) or isinstance(d[f], datetime):
                d[f] = d[f].strftime("%m/%d/%Y")
    return dl

def writeData(dl : List[dict], fname : str, fdir : str = None):
    if fdir is None:
        fdir = os.path.join(os.path.dirname(__file__), "data")
    if len(dl) > 0:
        with open(os.path.join(fdir, fname), "wt") as fp:
            json.dump(dl, fp, indent=4)

def readData(fname : str, fdir : str = None) -> List[dict]:
    if fdir is None:
        fdir = os.path.join(os.path.dirname(__file__), "data")
    with open(os.path.join(fdir, fname), "rt") as fp:
        return json.load(fp)