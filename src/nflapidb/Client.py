import os
from typing import List
import nflapi.Client
from nflapidb.EntityManager import EntityManager
import nflapidb.Utilities as util
from nflapidb.TeamManagerFacade import TeamManagerFacade
from nflapidb.RosterManagerFacade import RosterManagerFacade
from nflapidb.ScheduleManagerFacade import ScheduleManagerFacade
from nflapidb.PlayerProfileManagerFacade import PlayerProfileManagerFacade
from nflapidb.PlayerGamelogManagerFacade import PlayerGamelogManagerFacade
from nflapidb.GameSummaryManagerFacade import GameSummaryManagerFacade
from nflapidb.GameScoreManagerFacade import GameScoreManagerFacade
from nflapidb.GameDriveManagerFacade import GameDriveManagerFacade
from nflapidb.GamePlayManagerFacade import GamePlayManagerFacade

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
        self._team_mgr = None
        self._roster_mgr = None
        self._sched_mgr = None
        self._plprof_mgr = None
        self._plgmlg_mgr = None
        self._gmsum_mgr = None
        self._gmscr_mgr = None
        self._gmdrv_mgr = None
        self._gmplay_mgr = None

    async def sync(self):
        dmgrs = [
            self._teamManager, self._rosterManager, self._scheduleManager,
            self._playerProfileManager, self._playerGamelogManager, self._gameSummaryManager,
            self._gameScoreManager, self._gameDriveManager, self._gamePlayManager
        ]
        for mgr in dmgrs:
            await mgr.sync()

    async def getTeams(self, teams : List[str] = None) -> List[dict]:
        return await self._teamManager.find(teams)

    async def getRoster(self, teams : List[str] = None,
                        positions : List[str] = None,
                        last_names : List[str] = None,
                        first_names : List[str] = None,
                        profile_ids : List[int] = None,
                        include_previous_teams : bool = False) -> List[dict]:
        return await self._rosterManager.find(teams=teams, positions=positions,
                                              last_names=last_names, first_names=first_names,
                                              profile_ids=profile_ids,
                                              include_previous_teams=include_previous_teams)

    @property
    def _entityManager(self) -> EntityManager:
        return self._entity_manager

    @property
    def _apiClient(self) -> nflapi.Client.Client:
        return self._nflapi

    @property
    def _teamManager(self) -> TeamManagerFacade:
        if self._team_mgr is None:
            self._team_mgr = TeamManagerFacade(self._entityManager,
                                               self._apiClient)
        return self._team_mgr

    @property
    def _rosterManager(self) -> RosterManagerFacade:
        if self._roster_mgr is None:
            self._roster_mgr = RosterManagerFacade(self._entityManager,
                                                   self._apiClient,
                                                   self._teamManager)
        return self._roster_mgr

    @property
    def _scheduleManager(self) -> ScheduleManagerFacade:
        if self._sched_mgr is None:
            self._sched_mgr = ScheduleManagerFacade(self._entityManager,
                                                    self._apiClient)
        return self._sched_mgr

    @property
    def _playerProfileManager(self) -> PlayerProfileManagerFacade:
        if self._plprof_mgr is None:
            self._plprof_mgr = PlayerProfileManagerFacade(self._entityManager,
                                                          self._apiClient,
                                                          self._rosterManager)
        return self._plprof_mgr

    @property
    def _playerGamelogManager(self) -> PlayerGamelogManagerFacade:
        if self._plgmlg_mgr is None:
            self._plgmlg_mgr = PlayerGamelogManagerFacade(self._entityManager,
                                                          self._apiClient,
                                                          self._rosterManager)
        return self._plgmlg_mgr

    @property
    def _gameSummaryManager(self) -> GameSummaryManagerFacade:
        if self._gmsum_mgr is None:
            self._gmsum_mgr = GameSummaryManagerFacade(self._entityManager,
                                                       self._apiClient,
                                                       self._scheduleManager,
                                                       self._rosterManager,
                                                       self._teamManager)
        return self._gmsum_mgr

    @property
    def _gameScoreManager(self) -> GameScoreManagerFacade:
        if self._gmscr_mgr is None:
            self._gmscr_mgr = GameScoreManagerFacade(self._entityManager,
                                                     self._apiClient,
                                                     self._scheduleManager)
        return self._gmscr_mgr

    @property
    def _gameDriveManager(self) -> GameDriveManagerFacade:
        if self._gmdrv_mgr is None:
            self._gmdrv_mgr = GameDriveManagerFacade(self._entityManager,
                                                     self._apiClient,
                                                     self._scheduleManager)
        return self._gmdrv_mgr

    @property
    def _gamePlayManager(self) -> GamePlayManagerFacade:
        if self._gmplay_mgr is None:
            self._gmplay_mgr = GamePlayManagerFacade(self._entityManager,
                                                     self._apiClient,
                                                     self._scheduleManager,
                                                     self._rosterManager,
                                                     self._teamManager)
        return self._gmplay_mgr