from nflapidb.Entity import Entity, Index

class game_summary(Entity):
    @Index
    def gsis_id(self):
        return "str"

    @Index
    def team(self):
        return "str"

    @Index
    def player_id(self):
        return "str"

    @Index
    def profile_id(self):
        return "str"
