from nflapidb.Entity import Entity, PrimaryKey, Index

class game_play(Entity):
    @PrimaryKey
    def gsis_id(self):
        return "str"

    @PrimaryKey
    def drive_id(self):
        return "int"

    @PrimaryKey
    def play_id(self):
        return "int"

    @PrimaryKey
    def sequence(self):
        return "int"

    @Index
    def team(self):
        return "str"

    @Index
    def player_id(self):
        return "str"

    @Index
    def profile_id(self):
        return "str"

    @Index
    def stat_id(self):
        return "int"

    @Index
    def stat_cat(self):
        return "str"
