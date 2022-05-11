# from curses.ascii import SI
from tkinter.tix import Tree
from numpy import empty
from sqlalchemy import create_engine, false, true
import sys
import pandas as pd
import json


class Maria:
    def __init__(self, dataf):
        self.dataf = dataf
        try:
            sqlEngine = create_engine(
                "mysql+pymysql://it:Acumen321@192.168.5.238/Ace", pool_recycle=3600
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

    def GetStudentInfo(self,Sid,condition="",field="*") -> pd.DataFrame:
        # condition = ""
        # field = "*"
        if len(Sid) > 0:
            condition = "WHERE ID='%s'" % Sid
        sql = "SELECT "+field+" FROM StudentInfo " + condition
        studentinfo = pd.read_sql(sql, self.cursor)
        return studentinfo

    # def ImportStudentInfo(self):

    #     # import student info function
    #     studentondb = self.GetStudentInfo(Sid="")
    #     studentInfo = self.dataf.drop(
    #         columns=[
    #             "Section",
    #             "Campus_code",
    #             "Term",
    #             "TermYear",
    #             "Semester",
    #             "Program_code",
    #         ]
    #     ).rename(columns={"StudentID": "ID"})
    #     droplist = studentondb["ID"].tolist()
    #     differ = (
    #         pd.concat([studentInfo, studentondb])
    #         .drop_duplicates(subset=["ID"])
    #         .set_index("ID")
    #         .drop(droplist)
    #     )
    #     differ.to_sql(name="StudentInfo", con=self.cursor, if_exists="append")

    def ImportStuInfo(self):
        studentInfo = self.dataf.drop(
            columns=[
                "Section",
                "Campus_code",
                "Term",
                "TermYear",
                "Semester",
                "Program_code",
            ]
        ).rename(columns={"StudentID": "ID"})
        # index = 0
        print(studentInfo)
        for index in range(2):
            stuid = studentInfo.iloc[index]['ID']
            infoDB = self.GetStudentInfo(Sid=stuid).drop(columns=['EmergencyContact','Status']).set_index('ID')
            print("Database output :" )
            print(infoDB)
            print("File Output :" )
            
            stuinfo = studentInfo.iloc[[index]].set_index('ID')
            # ! Birthday Not match !!
            print(stuinfo['Birthday'])
            print(pd.concat([infoDB,stuinfo]).duplicated(subset=['Address']))
            # if (pd.concat([infoDB,studentInfo.iloc[[index]]]).duplicated() is not empty) :
            #     print(pd.concat([infoDB,studentInfo.iloc[[index]]]).duplicated())
            
            # studentInfo.iloc[[index]]
            
    def ImportStudentProgramInfo(self):

        studentproinfo = self.dataf.drop(
            columns=[
                "Fullname",
                "Firstname",
                "Lastname",
                "CampEmail",
                "HomeEmail",
                "Gender",
                "Birthday",
                "Address",
                "Country",
                "ContactInfo",
            ],
            errors="ignore",
        ).set_index("StudentID")
        studentproinfo.to_sql(
            name="StudentProgram", con=self.cursor, if_exists="append"
        )
 