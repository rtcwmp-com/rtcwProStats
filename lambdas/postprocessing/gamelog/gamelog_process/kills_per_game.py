from gamelog_process.award_class import AwardClass

"""
relevant json element
{
    match_id: "1630953060",
    round_id: "1",
    unixtime: "1630953652",
    group: "player",
    label: "kill",
    agent: "68deaefc0a07be79fcb2cc5104a71b1e",
    other: "5379320f3c64f43cdaf3350fc13011ce",
    weapon: "MP-40",
    other_health: 125,
    agent_pos: "589.451965,-70.798233,69.000000",
    agent_angle: "79.161987",
    other_pos: "2791.199219,4.931318,188.575974",
    other_angle: "-88.071899",
    allies_alive: "5",
    axis_alive: "1"
}
"""


class KillsPerGame(AwardClass):
    """Calculate the longest kill in the game."""

    award_name = "Kills Per Game"

    def __init__(self):
        super().__init__(self.award_name)

    def process_event(self, rtcw_event):
        """Take incoming kill and add it to a killer."""
        try:
            if rtcw_event.get("label", None) == "kill":
                killer = rtcw_event.get("agent", "no guid")
                self.players_values[killer] = self.players_values.get(killer, 0) + 1
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            error_msg = template.format(type(ex).__name__, ex.args)
            if self.debug:
                print("error at " + self.__name__ + " process event\n")
                print(error_msg)


