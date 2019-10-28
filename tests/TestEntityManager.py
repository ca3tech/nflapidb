import unittest
import unittest.mock
import asyncio
import os
import json
from motor.motor_asyncio import AsyncIOMotorCollection
from nflapidb.EntityManager import EntityManager

class TestEntityManager(unittest.TestCase):

    def setUp(self):
        self.entmgr = None

    def tearDown(self):
        if self.entmgr is not None:
            self.entmgr.dispose()

    def _run(self, coro):
        return asyncio.get_event_loop().run_until_complete(coro)

    def test__connect(self):
        self.entmgr = MockEntityManager()
        self.assertTrue(self.entmgr.connectCalled)

    def test__primaryKey__id_only(self):
        mc = unittest.mock.create_autospec(AsyncIOMotorCollection)
        async def ii():
            return [{"_id_": {"key": [("_id", 1)]}}]
        mc.index_information = ii
        self.entmgr = MockEntityManager()
        self.assertEqual(self._run(self.entmgr._primaryKey(mc)), ["_id"])

    def test__primaryKey__id_single_unique(self):
        mc = unittest.mock.create_autospec(AsyncIOMotorCollection)
        async def ii():
            return [
                {"_id_": {"key": [("_id", 1)]}},
                {"x_1": {"unique": True, "key": [("x", 1)]}}
            ]
        mc.index_information = ii
        self.entmgr = MockEntityManager()
        self.assertEqual(self._run(self.entmgr._primaryKey(mc)), ["x"])

    def test__primaryKey__id_two_unique(self):
        mc = unittest.mock.create_autospec(AsyncIOMotorCollection)
        async def ii():
            return [
                {"_id_": {"key": [("_id", 1)]}},
                {"x_1": {"unique": True, "key": [("x", 1), ("y", 1)]}}
            ]
        mc.index_information = ii
        self.entmgr = MockEntityManager()
        self.assertEqual(self._run(self.entmgr._primaryKey(mc)), ["x", "y"])

    def test__buildQuery__id_one_item(self):
        self.entmgr = MockEntityManager()
        data = [{"_id": 1, "col1": "A", "col2": 2}]
        self.assertEqual(self.entmgr._buildQuery(data, ["_id"]), {"_id": 1})

    def test__buildQuery__id_two_item(self):
        self.entmgr = MockEntityManager()
        data = [
            {"_id": 1, "col1": "A", "col2": 2},
            {"_id": 2, "col1": "A", "col2": 3}
        ]
        self.assertEqual(self.entmgr._buildQuery(data, ["_id"]), {"$or": [{"_id": 1}, {"_id": 2}]})

    def test__buildQuery__2key_one_item(self):
        self.entmgr = MockEntityManager()
        data = [{"_id": 1, "col1": "A", "col2": 1, "col3": 1.0}]
        self.assertEqual(self.entmgr._buildQuery(data, ["col1", "col2"]), {"$and": [{"col1": "A"}, {"col2": 1}]})

    def test__buildQuery__2key_two_item(self):
        self.entmgr = MockEntityManager()
        data = [
            {"_id": 1, "col1": "A", "col2": 1, "col3": 1.0},
            {"_id": 2, "col1": "B", "col2": 1, "col3": 1.0}
        ]
        self.assertEqual(self.entmgr._buildQuery(data, ["col1", "col2"]),
                         {"$or": [{"$and": [{"col1": "A"}, {"col2": 1}]},
                                  {"$and": [{"col1": "B"}, {"col2": 1}]}]})

    def test_save_inserts(self):
        self.entmgr = EntityManager()
        ename = "ut_table1"
        data = [{"column1": "A", "column2": 1}, {"column1": "B", "column2": 2}]
        self._run(self.entmgr.save(ename, data))
        async def verify():
            db = self.entmgr._database
            self.assertTrue(ename in await db.list_collection_names(), f"collection {ename} not created")
            try:
                col = db[ename]
                self.assertEqual(await col.count_documents({}), 2, "document count not expected")
                dbdata = [_ async for _ in col.find()]
                self.assertEqual(dbdata, data, "data not expected")
            except Exception as e:
                self.assertTrue(False, f"Error during verification: {e}")
            finally:
                await db.drop_collection(col)
        self._run(verify())

    def test_save_updates_with_id(self):
        self.entmgr = EntityManager()
        ename = "ut_table1"
        data = [{"column1": "A", "column2": 1}, {"column1": "B", "column2": 2}]
        self._run(self.entmgr.save(ename, data))
        urec = data[1]
        urec["column2"] = 3
        try:
            self._run(self.entmgr.save(ename, [urec]))
        except Exception as e:
            try:
                db = self.entmgr._database
                db.drop_collection(db[ename])
            except Exception:
                pass
            self.assertTrue(False, f"Error during save: {e}")
            raise e
        async def verify():
            db = self.entmgr._database
            self.assertTrue(ename in await db.list_collection_names(), f"collection {ename} not created")
            try:
                col = db[ename]
                self.assertEqual(await col.count_documents({}), 2, "document count not expected")
                dbdata = [_ async for _ in col.find()]
                self.assertEqual(dbdata, data, "data not expected")
            except Exception as e:
                self.assertTrue(False, f"Error during verification: {e}")
            finally:
                await db.drop_collection(col)
        self._run(verify())

    def test_save_updates_with_multikey(self):
        # Create a configuration file for the test entity
        # This file will be read by the EntityManager when creating the collection
        ename = "ut_table1"
        entcfg = {
            "indices": [
                {"name": f"{ename}_uk", "columns": {"column1": 1, "column2": 1}, "unique": True}
            ]
        }
        entcfgdp = os.path.join(os.path.dirname(__file__), "data", "entities")
        entcfgfp = os.path.join(entcfgdp, ename)
        with open(entcfgfp, "wt") as fp:
            json.dump(entcfg, fp)
        try:
            self.entmgr = EntityManager(entityDirPath=entcfgdp)
            data = [{"column1": "A", "column2": 1, "column3": 1.0}, {"column1": "A", "column2": 2, "column3": 1.0},
                    {"column1": "B", "column2": 1, "column3": 1.0}, {"column1": "B", "column2": 2, "column3": 1.0}]
            self._run(self.entmgr.save(ename, data))
            urecs = [data[1], data[3]]
            urecs[0]["column3"] = 2.0
            urecs[1]["column3"] = 2.0
            try:
                self._run(self.entmgr.save(ename, urecs))
            except Exception as e:
                try:
                    db = self.entmgr._database
                    db.drop_collection(db[ename])
                except Exception:
                    pass
                self.assertTrue(False, f"Error during save: {e}")
                raise e
            async def verify():
                db = self.entmgr._database
                self.assertTrue(ename in await db.list_collection_names(), f"collection {ename} not created")
                try:
                    col = db[ename]
                    self.assertEqual(await col.count_documents({}), 4, "document count not expected")
                    dbdata = [_ async for _ in col.find()]
                    self.assertEqual(dbdata, data, "data not expected")
                except Exception as e:
                    self.assertTrue(False, f"Error during verification: {e}")
                finally:
                    await db.drop_collection(col)
            self._run(verify())
        finally:
            os.remove(entcfgfp)

class MockEntityManager(EntityManager):
    @property
    def connectCalled(self):
        return self._connect_called

    def __init__(self):
        self._connect_called = False
        super(MockEntityManager, self).__init__()

    def _connect(self):
        self._connect_called = True
        super(MockEntityManager, self)._connect()