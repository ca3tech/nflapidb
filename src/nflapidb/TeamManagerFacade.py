from typing import List
import logging
import nflapi.Client
from nflapidb.EntityManager import EntityManager
from nflapidb.DataManagerFacade import DataManagerFacade
from nflapidb.QueryModel import QueryModel, Operator
import nflapidb.Utilities as util

class TeamManagerFacade(DataManagerFacade):

    def __init__(self, entityManager : EntityManager, apiClient : nflapi.Client.Client = None):
        super(TeamManagerFacade, self).__init__("team", entityManager, apiClient)

    async def sync(self) -> List[dict]:
        logging.info("Syncing team data...")
        nteams = await self._findNewTeams()
        logging.info("Saving {} teams...".format(len(nteams)))
        return await self.save(nteams)

    async def find(self, teams : List[str] = None) -> List[dict]:
        return await super(TeamManagerFacade, self).find(teams=teams)

    async def delete(self, teams : List[str] = None) -> List[dict]:
        return await super(TeamManagerFacade, self).delete(teams=teams)

    def _getQueryModel(self, **kwargs) -> QueryModel:
        qm = QueryModel()
        if "teams" in kwargs and kwargs["teams"] is not None:
            qm.cstart("team", kwargs["teams"], Operator.IN)
        return qm
    
    async def _findNewTeams(self) -> List[dict]:
        logging.info("Retrieving teams from NFL API...")
        teams = self._apiClient.getTeams(active_only=False)
        cteams = await self.find()
        if len(cteams) > 0:
            tdiffs = util.ddquery(cteams, teams)
            teams = tdiffs[1]
        return teams