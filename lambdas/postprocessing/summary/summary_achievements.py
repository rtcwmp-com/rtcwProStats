class Achievements:
    """Calculate the longest kill in the game."""

    def __init__(self, stats, wstats):
        self.potential_achievements = {}
        
        self.get_killpeak_lines(stats)
        self.get_combat_medic_lines(stats)
        self.get_combat_engineer_lines(stats)
        self.get_combat_lt_lines(stats, wstats)


    def get_killpeak_lines(self, stats):
        """Return result as {"Award name": {"guid":"value"}}."""
        """Simply filter down killpeaks to 6"""
        temp_achievements = {}
        for guid, player_stat in stats.items():
            if int(player_stat.get("categories", {}).get("killpeak",0)) > 2:
                temp_achievements[guid] = int(player_stat.get("categories", {}).get("killpeak",0))            
        self.potential_achievements["Killpeak"] = temp_achievements
        
    def get_combat_medic_lines(self, stats):
        """Return result as {"Award name": {"guid":"value"}}."""
        """Determine combat medic achievement.
        
        This achievement requires a player to get a certain number of kills, gibs, revives.
        21/7/7 for gold (level 7 and above)
        18/6/6 for silver (level 6)
        15/5/5 for bronze (level 5)
        12/4/4 for rusted iron (level 4)
        """
        temp_achievements = {}
        for guid, player_stat in stats.items():
            kills = int(player_stat.get("categories", {}).get("kills",0))
            gibs = int(player_stat.get("categories", {}).get("gibs",0))
            revives = int(player_stat.get("categories", {}).get("revives",0))
            if  kills >= 12 and gibs >=4 and revives>=4: # to skip some cycles
                if  kills >= 30 and gibs >=10 and revives>=10:
                    temp_achievements[guid] = 10
                    continue
                if  kills >= 27 and gibs >=9 and revives>=9:
                    temp_achievements[guid] = 9
                    continue
                if  kills >= 24 and gibs >=8 and revives>=8:
                    temp_achievements[guid] = 8
                    continue
                if  kills >= 21 and gibs >=7 and revives>=7:
                    temp_achievements[guid] = 7
                    continue
                if  kills >= 18 and gibs >=6 and revives>=6:
                    temp_achievements[guid] = 6
                    continue
                if  kills >= 15 and gibs >=5 and revives>=5:
                    temp_achievements[guid] = 5
                    continue
                if  kills >= 12 and gibs >=4 and revives>=4:
                    temp_achievements[guid] = 4
                    continue
                #pretty much i dont feel like doing math in a loop today
        self.potential_achievements["Combat Medic"] = temp_achievements
        
    def get_combat_engineer_lines(self, stats):
        """Return result as {"Award name": {"guid":"value"}}."""
        """Determine combat engineer achievement.
        
        This achievement requires a player to get a certain number of plans+defuses, objectives capture.
        1 point for obj cap 
        1 point for destroy or defuse
        1 point for 8 kills
        """
        temp_achievements = {}
        for guid, player_stat in stats.items():
            kills = int(player_stat.get("categories", {}).get("kills",0))
            destroy = int(player_stat.get("categories", {}).get("obj_destroyed",0))
            defuse = int(player_stat.get("categories", {}).get("dyn_defused",0))
            obj_cap = int(player_stat.get("categories", {}).get("obj_captured",0))
            obj_take = int(player_stat.get("categories", {}).get("obj_taken",0))

            if  destroy > 0: # at least one destroy event is required to be engineer!
                temp_achievements[guid] = temp_achievements.get(guid,0) + destroy
                temp_achievements[guid] = temp_achievements.get(guid,0) + defuse
                if obj_take > 0: # RTCW Pro issue where multiple people are credited with a cap
                    temp_achievements[guid] = temp_achievements.get(guid,0) + obj_cap
                
                temp_achievements[guid] = temp_achievements.get(guid,0) + int(kills/8)
            
            if guid in temp_achievements and temp_achievements.get(guid,0) < 4: # ignore small achievements to save noise/costs
                del temp_achievements[guid]
                
        self.potential_achievements["Combat Engineer"] = temp_achievements
    
    def get_combat_lt_lines(self, stats, wstats):
        """Return result as {"Award name": {"guid":"value"}}."""
        """Determine Lieutenant Colonel achievement.
        
        This achievement requires a player to get a certain number of kills, smoke kills, and ammo packs.
        21/7/7 for gold (level 7 and above)
        18/6/6 for silver (level 6)
        15/5/5 for bronze (level 5)
        12/4/4 for rusted iron (level 4)
        """
        temp_achievements = {}
        for guid, player_stat in stats.items():
            kills = int(player_stat.get("categories", {}).get("kills",0))
            ammo_given = int(player_stat.get("categories", {}).get("ammogiven",0))
            
            artillery = wstats.get(guid,{}).get("Artillery",{}).get("kills",0)
            airstrike = wstats.get(guid,{}).get("Airstrike",{}).get("kills",0)
            
            air_support = artillery + airstrike
            
            if  kills >= 12 and air_support >=4 and ammo_given>=8: # to skip some cycles
                if  kills >= 30 and air_support >=10 and ammo_given>=20:
                    temp_achievements[guid] = 10
                    continue
                if  kills >= 27 and air_support >=9 and ammo_given>=18:
                    temp_achievements[guid] = 9
                    continue
                if  kills >= 24 and air_support >=8 and ammo_given>=16:
                    temp_achievements[guid] = 8
                    continue
                if  kills >= 21 and air_support >=7 and ammo_given>=14:
                    temp_achievements[guid] = 7
                    continue
                if  kills >= 18 and air_support >=6 and ammo_given>=12:
                    temp_achievements[guid] = 6
                    continue
                if  kills >= 15 and air_support >=5 and ammo_given>=10:
                    temp_achievements[guid] = 5
                    continue
                if  kills >= 12 and air_support >=4 and ammo_given>=8:
                    temp_achievements[guid] = 4
                    continue
            self.potential_achievements["Lieutenant Colonel"] = temp_achievements
    
# debug
# self = Achievements(stats, wstats)
# self.potential_achievements
       
    



