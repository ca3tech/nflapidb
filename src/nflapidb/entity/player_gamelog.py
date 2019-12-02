from nflapidb.Entity import Entity, PrimaryKey, Column, Index

class player_gamelog(Entity):
    @PrimaryKey
    def profile_id(self):
        return "int"

    @PrimaryKey
    def season(self):
        return "int"

    @PrimaryKey
    def season_type(self):
        return "str"

    @PrimaryKey
    def wk(self):
        return "int"

    @Column
    def game_date(self):
        return "datetime"

    @Index
    def previous_teams(self):
        return "list"
