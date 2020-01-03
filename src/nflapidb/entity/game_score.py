from nflapidb.Entity import Entity, PrimaryKey

class game_score(Entity):
    @PrimaryKey
    def gsis_id(self):
        return "str"

    @PrimaryKey
    def team(self):
        return "str"
