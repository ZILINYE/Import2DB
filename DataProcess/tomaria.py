# from msilib.schema import Error
from sqlalchemy import create_engine
import sys
import pandas as pd
import json
import difflib


class Maria:
    def __init__(self, dataf):
        self.dataf = dataf
        try:
            sqlEngine = create_engine(
                "mysql+pymysql://root:YZLmm1994@192.168.3.73/Test", pool_recycle=3600
            )

        except:
            print(f"Error connecting to MariaDB Platform: {e}")
            print("error on connecting to mysql")
            sys.exit(1)

        # Get Cursor
        self.cursor = sqlEngine.connect()

    def GetProgramInfo(self, Pcode) -> pd.DataFrame:
        condition = ""
        if len(Pcode) > 0:
            condition = "WHERE Program_code='%s'" % Pcode
        sql = "SELECT Program_code,Program_desc FROM ProgramInfo " + condition
        programinfo = pd.read_sql(sql, self.cursor)
        return programinfo
        # print(df)
        # result = df.to_json(orient="records")
        # parsed = json.loads(result)
        # jsonlist = json.dumps(parsed, indent=4)
        # print(jsonlist)
    def GetStudentInfo(self,Sid) -> pd.DataFrame: 
        condition = ""
        if len(Sid) > 0:
            condition = "WHERE ID='%s'" % Sid
        sql = "SELECT * FROM StudentInfo " + condition
        studentinfo = pd.read_sql(sql, self.cursor)
        return studentinfo

    def ImportStudentInfo(self):
        # pd.set_option('display.max_columns', 500)
        # pd.set_option('display.width', None)
        # pd.set_option('display.max_colwidth', None)

       
        # import student info function
        studentondb = self.GetStudentInfo(Sid='')
        studentInfo = self.dataf.drop(columns=['Section','Campus_code','Term','TermYear','Semester','Program_code']).rename(columns={'StudentID':'ID'})
        droplist= studentondb['ID'].tolist()
        differ = pd.concat([studentInfo,studentondb]).drop_duplicates(subset=['ID']).set_index('ID').drop(droplist)
        differ.to_sql(name='StudentInfo',con=self.cursor,if_exists='append')
        # print(studentInfo)
    def ImportStudentProgramInfo(self):


        studentproinfo = self.dataf.drop(columns=['Fullname','Firstname','Lastname','CampEmail','HomeEmail','Gender','Birthday','Address','Country','ContactInfo'],errors='ignore').set_index('StudentID')
        studentproinfo.to_sql(name='StudentProgram',con=self.cursor,if_exists='append')