import os
from typing import List
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase, AsyncIOMotorCollection
from pymongo import ReturnDocument, IndexModel, ASCENDING
from pymongo.errors import InvalidName
from bson.codec_options import CodecOptions
import importlib
from datetime import datetime
import dateutil.parser
import nflapidb.Utilities as util
from nflapidb.Entity import Entity

class EntityManager:

    def __init__(self, dbHost=os.environ["DB_HOST"], dbPort=int(os.environ["DB_PORT"]),
                 dbAuthName=(os.environ["DB_AUTH_NAME"] if "DB_AUTH_NAME" in os.environ else ""),
                 dbName=os.environ["DB_NAME"],
                 dbUser=os.environ["DB_USER"], dbUserPwd=os.environ["DB_USER_PWD"],
                 dbSSL=("DB_USE_SSL" in os.environ and util.str2bool(os.environ["DB_USE_SSL"])),
                 dbReplicaSet=(os.environ["DB_REPL_SET"] if "DB_REPL_SET" in os.environ else ""),
                 dbAppName=(os.environ["DB_APP_NAME"] if "DB_APP_NAME" in os.environ else ""),
                 entityDirPath: str = None):
        """Create a new EntityManager object"""
        self._db_host = dbHost
        self._db_port = dbPort
        if not (dbAuthName is None or dbAuthName == ""):
            self._db_auth_name = dbAuthName
        else:
            self._db_auth_name = dbName
        self._db_name = dbName
        self._db_user = dbUser
        self._db_user_pwd = dbUserPwd
        self._conn = None
        self._db = None
        if entityDirPath is None:
            entityDirPath = os.path.join(os.path.dirname(__file__), "entity")
        self._entity_dir_path = entityDirPath
        self._ssl = dbSSL
        self._repl_set = dbReplicaSet
        self._app_name = dbAppName
        self._entityCache = {}
        self._connect()

    def dispose(self):
        """Call this when you are done using the object"""
        self._conn.close()

    def getEntity(self, entityName: str) -> Entity:
        ent = None
        if entityName in self._entityCache:
            ent = self._entityCache[entityName]
        else:
            efpath = os.path.join(os.path.relpath(self._entity_dir_path), f"{entityName}.py")
            if os.path.exists(efpath):
                impath = efpath.replace("/", ".").replace(".py", "")
                entmod = importlib.import_module(impath)
                # There has to be a better way of instantiating the object then this
                ent = eval(f"entmod.{entityName}()")
                self._entityCache[entityName] = ent
        return ent

    async def save(self, entityName: str, data: List[dict]) -> List[dict]:
        col = await self._getCollection(entityName)
        pkeys = await self._primaryKey(col)
        for i in range(0, len(data)):
            datum = self._applyAttributeTypes(data[i], entityName)
            q = self._buildQueryItem(datum, pkeys)
            data[i] = await col.find_one_and_replace(q, datum, upsert=True, return_document=ReturnDocument.AFTER)
        return data

    async def find(self, entityName: str, query: dict=None, projection: dict=None, collection : AsyncIOMotorCollection=None) -> List[dict]:
        if collection is None:
            collection = await self._getCollection(entityName)
        return [d async for d in collection.find(query, projection=projection)]

    async def delete(self, entityName: str, query: dict={}, collection : AsyncIOMotorCollection=None) -> int:
        if collection is None:
            collection = await self._getCollection(entityName)
        rslt = await collection.delete_many(query)
        return rslt.deleted_count

    async def drop(self, entityName: str):
        await self._database.drop_collection(entityName)

    @property
    def _entity_dir_path(self) -> str:
        return self._edpath

    @_entity_dir_path.setter
    def _entity_dir_path(self, path : str):
        self._edpath = path

    def _applyAttributeTypes(self, datum : dict, entityName : str) -> dict:
        def dtparse(dt : any) -> datetime:
            # The US timezone abbreviations from https://www.timetemperature.com/abbreviations/united_states_time_zone_abbreviations.shtml
            tzi = {
                "AST": -14400,
                "EDT": -14400,
                "EST": -18000,
                "CDT": -18000,
                "CST": -21600,
                "MDT": -21600,
                "MST": -25200,
                "PDT": -25200,
                "PST": -28800,
                "AKDT": -28800,
                "AKST": -32400,
                "HADT": -32400,
                "HAST": -36000,
                "HST": -36000,
                "SDT": -36000,
                "SST": -39600,
                "CHST": -36000
            }
            if isinstance(dt, str):
                if dt is not None:
                    if dt == "":
                        dt = None
                    else:
                        dt = dateutil.parser.parse(dt, tzinfos=tzi)
            return dt
        switch = {
            "int": int,
            "float": float,
            "datetime": dtparse
        }
        ent = self.getEntity(entityName)
        if ent is not None:
            for cname in datum:
                if datum[cname] is not None:
                    ctype = ent.columnType(cname)
                    if ctype is not None:
                        if ctype in switch:
                            datum[cname] = switch[ctype](datum[cname])
        return datum

    async def _getCollection(self, entityName: str) -> AsyncIOMotorCollection:
        db = self._database
        if len(await db.list_collection_names(filter={"name": entityName})) > 0:
            col = db[entityName]
        else:
            db.create_collection(entityName, codec_options=CodecOptions(tz_aware=True))
            col = db[entityName]
            await self._createCollectionIndices(col)
        return col

    def _getCollectionIndices(self, entityName: str) -> List[IndexModel]:
        ixl = None
        ent = self.getEntity(entityName)
        if ent is not None:
            if len(ent.primaryKey) > 0:
                ixl = [IndexModel([(k, ASCENDING) for k in ent.primaryKey], unique=True)]
            if len(ent.indices) > 0:
                if ixl is None:
                    ixl = []
                ixl.extend([IndexModel([(k, ASCENDING) for k in ent.indices])])
        return ixl

    async def _createCollectionIndices(self, collection : AsyncIOMotorCollection):
        indices = self._getCollectionIndices(collection.name)
        if indices is not None:
            await collection.create_indexes(indices)

    async def _primaryKey(self, collection : AsyncIOMotorCollection) -> list:
        cnames = ["_id"]
        allxcnames = []
        idxmd = await collection.index_information()
        if not isinstance(idxmd, list):
            idxmd = [idxmd]
        for idx in idxmd:
            for idxname in idx:
                if idxname == "_id_" or "unique" in idx[idxname]:
                    # _id and unique keys are candidates
                    allxcnames.append([t[0] for t in idx[idxname]["key"]])
        if len(allxcnames) == 1:
            # _id was the only index and so it is the key
            cnames = allxcnames[0]
        elif len(allxcnames) > 1:
            # choose the first set of columns that are not the _id column
            fi = [i for i in range(0, len(allxcnames)) if allxcnames[i] != ["_id"]]
            cnames = allxcnames[fi[0]]
        return cnames

    def _buildQuery(self, data : List[dict], colnames : List[str]) -> List[dict]:
        q = [self._buildQueryItem(rec, colnames) for rec in data]
        if len(q) > 1:
            q = {"$or": q}
        else:
            q = q[0]
        return q

    def _buildQueryItem(self, datum : dict, colnames : List[str]) -> dict:
        colnames = list(set(colnames).intersection(set(datum.keys())))
        if len(colnames) == 0:
            colnames = list(datum.keys()) 
        if len(colnames) > 1:
            return {"$and": [dict(zip([k], [datum[k]])) for k in colnames]}
        else:
            return dict(zip(colnames, [datum[colnames[0]]]))

    @property
    def _connection(self) -> AsyncIOMotorClient:
        if self._conn is None:
            self._connect()
        return self._conn
    
    @property
    def _database(self) -> AsyncIOMotorDatabase:
        if self._db is None:
            try:
                self._db = self._connection[self._db_name]
                if self._db is None:
                    self._db = self._connection.get_database(self._db_name)
            except InvalidName:
                self._db = self._connection.get_database(self._db_name)
        return self._db

    def _connect(self):
        curi = f"mongodb://{self._db_user}:{self._db_user_pwd}@{self._db_host}:{self._db_port}/{self._db_auth_name}?retrywrites=false&maxIdleTimeMS=120000"
        if self._ssl:
            curi = f"{curi}&ssl=true"
        if not (self._repl_set is None or self._repl_set == ""):
            curi = f"{curi}&replicaSet={self._repl_set}"
        if not (self._app_name is None or self._app_name == ""):
            curi = f"{curi}&appName={self._app_name}"
        print(f"Mongo Connection URI: {curi}")
        self._conn = AsyncIOMotorClient(curi)