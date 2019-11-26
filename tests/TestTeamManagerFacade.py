import unittest
import asyncio
from nflapidb.TeamManagerFacade import TeamManagerFacade
from nflapidb.EntityManager import EntityManager
import nflapidb.Utilities as util

class TestTeamManagerFacade(unittest.TestCase):

    def setUp(self):
        self.entityName = "team"
        self.entmgr = EntityManager()
        self.datamgr = TeamManagerFacade(self.entmgr)

    def tearDown(self):
        util.runCoroutine(self.datamgr.drop())
        self.entmgr.dispose()

    def test_sync_initializes_collection(self):
        recs = util.runCoroutine(self.datamgr.sync())
        self.assertGreater(len(recs), 0, "sync returned 0 records")
        dbrecs = util.runCoroutine(self.entmgr.find(self.entityName))
        self.assertEqual(dbrecs, recs, "db records differ")

    def test_save_appends(self):
        recs = util.runCoroutine(self.datamgr.sync())
        arec = util.runCoroutine(self.datamgr.save([{"team": "NX"}]))
        recs.extend(arec)
        dbrecs = util.runCoroutine(self.entmgr.find(self.entityName))
        self.assertEqual(len(dbrecs), len(recs), "record count differs")
        self.assertEqual(dbrecs, recs, "db records differ")

    def test_find_no_constraints(self):
        recs = util.runCoroutine(self.entmgr.save(self.entityName, [{"team": "KC"}, {"team": "PIT"}]))
        for rec in recs:
            del rec["_id"]
        dbrecs = util.runCoroutine(self.datamgr.find())
        self.assertEqual(dbrecs, recs, "db records differ")

    def test_find_constraint(self):
        recs = util.runCoroutine(self.entmgr.save(self.entityName, [{"team": "KC"}, {"team": "PIT"}]))
        for rec in recs:
            del rec["_id"]
        dbrecs = util.runCoroutine(self.datamgr.find(teams=["KC"]))
        self.assertEqual(dbrecs, [recs[0]], "db records differ")