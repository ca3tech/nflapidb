import unittest
import unittest.mock
import asyncio
import os
import json
import importlib
from datetime import datetime
import pytz
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

    def assertDictPropEqual(self, d1 : dict, d2 : dict):
        k1 = set(d1.keys())
        k2 = set(d2.keys())
        self.assertEqual(k1, k2, "keys differ")
        for k in k1 & k2:
            v1 = d1[k]
            v2 = d2[k]
            if isinstance(v1, dict):
                self.assertDictPropEqual(v1, v2)
            elif isinstance(v1, list):
                self.assertDictListEqual(v1, v2)
            else:
                self.assertEqual(v1, v2, f"key {k} values differ")

    def assertDictListEqual(self, l1 : list, l2 : list):
        self.assertEqual(len(l1), len(l2), "list lengths differ")
        if len(l1) == len(l2):
            if not (isinstance(l1[0], dict) or isinstance(l1[0], list)):
                l1.sort()
                l2.sort()
            for i in range(0, len(l1)):
                if isinstance(l1[i], dict):
                    self.assertDictPropEqual(l1[i], l2[i])
                elif isinstance(l1[i], list):
                    self.assertDictListEqual(l1[i], l2[i])
                else:
                    self.assertEqual(l1[i], l2[i], f"list item {i} differs")

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

    def test__primaryKey_with_index(self):
        mc = unittest.mock.create_autospec(AsyncIOMotorCollection)
        async def ii():
            return [{"_id_": {"key": [("_id", 1)]}},
                    {"x_1": {"key": [("x", 1)]}}]
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
        self.assertDictPropEqual(self.entmgr._buildQuery(data, ["col1", "col2"]), {"$and": [{"col1": "A"}, {"col2": 1}]})

    def test__buildQuery__2key_two_item(self):
        self.entmgr = MockEntityManager()
        data = [
            {"_id": 1, "col1": "A", "col2": 1, "col3": 1.0},
            {"_id": 2, "col1": "B", "col2": 1, "col3": 1.0}
        ]
        self.assertDictPropEqual(self.entmgr._buildQuery(data, ["col1", "col2"]),
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

    def test_entity_instantiation(self):
        try:
            ename = "ut_table1"
            ut_table1 = importlib.import_module(f"tests.data.entities.{ename}")
            obj = eval(f"ut_table1.{ename}()")
            self.assertFalse(obj is None, "instantiation returned None")
            self.assertEqual(obj.primaryKey, set(["column1", "column2"]), "primaryKey mismatch")
        except Exception as e:
            self.assertEqual(e, None, "Error")

    def test_save_updates_with_multikey(self):
        ename = "ut_table1"
        entcfgdp = os.path.join(os.path.relpath(os.path.dirname(__file__)), "data", "entities")
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

    def test_save_converts_numeric_types(self):
        ename = "ut_table1"
        entcfgdp = os.path.join(os.path.relpath(os.path.dirname(__file__)), "data", "entities")
        self.entmgr = EntityManager(entityDirPath=entcfgdp)
        data = [{"column1": "A", "column2": "1", "column3": "1.0"}, {"column1": "A", "column2": "2", "column3": "1.0"},
                {"column1": "B", "column2": "1", "column3": "1.0"}, {"column1": "B", "column2": "2", "column3": "1.0"}]
        self._run(self.entmgr.save(ename, data))
        for i in range(0, len(data)):
            data[i]["column2"] = int(data[i]["column2"])
            data[i]["column3"] = float(data[i]["column3"])
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

    def test_db_preserves_timezone(self):
        ename = "ut_table2"
        entcfgdp = os.path.join(os.path.relpath(os.path.dirname(__file__)), "data", "entities")
        self.entmgr = EntityManager(entityDirPath=entcfgdp)
        tz = pytz.timezone("US/Eastern")
        data = [{"column1": "A", "column2": datetime(2019, 11, 20, 13, 00, tzinfo=tz)},
                {"column1": "B", "column2": datetime(2019, 11, 20, 16, 00, tzinfo=tz)}]
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

    def test_db_preserves_timezone_str(self):
        ename = "ut_table2"
        entcfgdp = os.path.join(os.path.relpath(os.path.dirname(__file__)), "data", "entities")
        self.entmgr = EntityManager(entityDirPath=entcfgdp)
        tz = pytz.timezone("US/Eastern")
        data = [{"column1": "A", "column2": datetime(2019, 11, 20, 13, 00, tzinfo=tz)},
                {"column1": "B", "column2": datetime(2019, 11, 20, 16, 00, tzinfo=tz)}]
        idata = data.copy()
        idata[0]["column2"] = idata[0]["column2"].strftime("%Y-%m-%d %H:%M %z")
        idata[1]["column2"] = idata[1]["column2"].strftime("%Y-%m-%d %H:%M %z")
        self._run(self.entmgr.save(ename, idata))
        async def verify():
            db = self.entmgr._database
            self.assertTrue(ename in await db.list_collection_names(), f"collection {ename} not created")
            try:
                col = db[ename]
                self.assertEqual(await col.count_documents({}), 2, "document count not expected")
                dbdata = []
                async for datum in col.find():
                    del datum["_id"]
                    datum["column2"] = pytz.utc.localize(datum["column2"]).astimezone(tz)
                    dbdata.append(datum)
                self.assertEqual(dbdata, data, "data not expected")
            except Exception as e:
                self.assertTrue(False, f"Error during verification: {e}")
            finally:
                await db.drop_collection(col)
        self._run(verify())

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