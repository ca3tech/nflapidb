import os
from typing import List
import nflapi.Client
from nflapidb.EntityManager import EntityManager
import nflapidb.Utilities as util

class Client:

    def __init__(self, dbHost=os.environ["DB_HOST"], dbPort=int(os.environ["DB_PORT"]),
                 dbAuthName=(os.environ["DB_AUTH_NAME"] if "DB_AUTH_NAME" in os.environ else ""),
                 dbName=os.environ["DB_NAME"],
                 dbUser=os.environ["DB_USER"], dbUserPwd=os.environ["DB_USER_PWD"],
                 dbSSL=("DB_USE_SSL" in os.environ and util.str2bool(os.environ["DB_USE_SSL"])),
                 dbReplicaSet=(os.environ["DB_REPL_SET"] if "DB_REPL_SET" in os.environ else ""),
                 dbAppName=(os.environ["DB_APP_NAME"] if "DB_APP_NAME" in os.environ else "")):
        self._entity_manager = EntityManager(dbHost, dbPort, dbAuthName, dbName, dbUser,
                                             dbUserPwd, dbSSL, dbReplicaSet, dbAuthName)
        self._nflapi = nflapi.Client.Client()

    def getTeams(self) -> List[dict]:
        entname = "team"
        teams = self._entity_manager.find(entname)
        if len(teams) == 0:
            teams = []
            for team in self._nflapi.getTeams():
                teams.append({entname: team})
            self._entity_manager.save(entname, teams)
        return teams

    def getRoster(self, teams : List[str]) -> List[dict]:
        entname = "roster"
        roster = self._entity_manager.find(entname, {"team": teams}, projection={"_id": False})
        if len(roster) == 0:
            roster = self._nflapi.getRoster(teams)
            self._entity_manager.save(entname, roster)
        return roster