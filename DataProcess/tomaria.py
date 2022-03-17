from msilib.schema import Error
from sqlalchemy import create_engine
import sys
import pandas as pd
import json

class Maria:
    def __init__(self, pd):
        self.pd = pd
        try:
            sqlEngine=create_engine('mysql+pymysql://it:Acumen321@192.168.5.235/Ace', pool_recycle=3600)
            
        except Error as e:
            print(f"Error connecting to MariaDB Platform: {e}")
            sys.exit(1)

        # Get Cursor
        self.cursor=sqlEngine.connect()

    def GetProgramInfo(self,Pcode):
        condition=''
        if len(Pcode)>0:
            condition = ("WHERE Program_code='%s'" % Pcode)
        sql = ("SELECT Program_code,Program_desc FROM ProgramInfo " + condition)
        df = pd.read_sql(sql,self.cursor)
        print(df)
        result = df.to_json(orient="records")
        parsed = json.loads(result)
        jsonlist = json.dumps(parsed, indent=4)
        print(jsonlist)

