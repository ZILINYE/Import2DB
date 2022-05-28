# from curses.ascii import SI
from distutils.log import error
import string

# from tkinter.tix import Tree
from numpy import empty
from sqlalchemy import create_engine, false, true
import sys
import pandas as pd

# import json
# from datetime import datetime
import mysql.connector


class Maria:
    def __init__(self, dataf):
        self.dataf = dataf
        try:
            sqlEngine = create_engine(
                "mysql+pymysql://it:Acumen321@192.168.5.238/Ace", pool_recycle=3600
            )
            self.mydb = mysql.connector.connect(
                host="192.168.5.238", user="it", password="Acumen321", database="Ace"
            )

        except:
            print(f"Error connecting to MariaDB Platform: {e}")
            print("error on connecting to mysql")
            sys.exit(1)

        # Get Cursor
        self.mycursor = self.mydb.cursor()
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

    def GetStudentInfo(self, Sid, condition="", field="*") -> pd.DataFrame:
        # condition = ""
        # field = "*"
        if len(Sid) > 0:
            condition = "WHERE ID='%s'" % Sid
        sql = "SELECT " + field + " FROM StudentInfo " + condition
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
        for index in range(len(studentInfo.index)):
            stuid = studentInfo.iloc[index]["ID"]
            infoDB = (
                self.GetStudentInfo(Sid=stuid)
                .drop(columns=["EmergencyContact", "Status"])
                .set_index("ID")
            )
            stuinfo = studentInfo.iloc[[index]].set_index("ID")
            # * convert datetime to string
            infoDB["Birthday"] = infoDB["Birthday"].astype(str)
            
            print("Woring on " + str(stuid))
            # If student already exist in database
            if len(pd.concat([infoDB, stuinfo]).duplicated().index) > 1:
                # todo call mysql query
                
                self.UpdateStuInfo(stuid=str(stuid), newinfo=stuinfo.fillna(''))
                # sys.exit(1)
            else:
                self.AddStuInfo(newinfo=stuinfo.fillna(''))
                # sys.exit(1)


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

    def AddStuInfo(self,newinfo):
        newinfo.to_sql(name="StudentInfo", con=self.cursor, if_exists="append")
    def UpdateStuInfo(self, stuid, newinfo):

        newinfosql = "Fullname = %s ,Firstname = %s ,CampEmail = %s , HomeEmail = %s , Gender = %s ,Birthday = %s ,Address = %s , Country = %s , ContactInfo = %s , Status ='Y' "
        condition = "WHERE ID = '%s'" % stuid
        sql = "UPDATE StudentInfo SET " + newinfosql+condition
        # newinfo =newinfo.fillna('')
        val = (newinfo.iloc[0]['Fullname'],newinfo.iloc[0]['Firstname'],newinfo.iloc[0]['CampEmail'],newinfo.iloc[0]['HomeEmail'],newinfo.iloc[0]['Gender'],newinfo.iloc[0]['Birthday'],newinfo.iloc[0]['Address'],newinfo.iloc[0]['Country'],newinfo.iloc[0]['ContactInfo'])
        self.mycursor.execute(sql,val)
        self.mydb.commit()
