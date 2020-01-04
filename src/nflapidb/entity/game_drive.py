from nflapidb.Entity import Entity, PrimaryKey, Index

class game_drive(Entity):
    @PrimaryKey
    def gsis_id(self):
        return "str"

    @PrimaryKey
    def drive_id(self):
        return "int"
