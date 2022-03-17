import mariadb
import sys


class Maria:
    def __init__(self, pd):
        self.pd = pd
        try:
            conn = mariadb.connect(
                user="it",
                password="Acumen@321",
                host="192.168.5.235",
                port=3306,
                database="Ace"

            )
        except mariadb.Error as e:
            print(f"Error connecting to MariaDB Platform: {e}")
            sys.exit(1)

        # Get Cursor
        self.cur = conn.cursor()

    def GetProgramInfo(self):
        ProgramList = self.cur.execute(
            "SELECT * FROM Campus")
        print(ProgramList)
        for (Program_code, Program_desc) in ProgramList:
            print(
                f"Program Code : {Program_code}, Program Description : {Program_desc}")
