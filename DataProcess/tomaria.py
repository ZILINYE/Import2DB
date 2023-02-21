from distutils.log import error
import string
from time import process_time_ns
from numpy import empty, int64
from sqlalchemy import create_engine, false, true
import sys
import pandas as pd
import mysql.connector


class Maria:
    def __init__(self, dataf):
        self.dataf = dataf
        try:
            sqlEngine = create_engine(
                "", pool_recycle=3600
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
        sql = "SELECT Program_code,Program_desc FROM Program " + condition
        programinfo = pd.read_sql(sql, self.cursor)
        return programinfo

    def GetStudentInfo(self, Sid, condition="", field="*") -> pd.DataFrame:
        # condition = ""
        # field = "*"
        if len(Sid) > 0:
            condition = "WHERE ID='%s'" % Sid
        sql = "SELECT " + field + " FROM Student " + condition
        studentinfo = pd.read_sql(sql, self.cursor)
        return studentinfo

    def GetEnrollment(self) -> pd.DataFrame:
        sql = "SELECT * FROM Enrollment "
        enrollment = pd.read_sql(sql, self.cursor)
        return enrollment

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


    def ImportEnrollment(self):

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
        )
        enrollment = self.GetEnrollment().drop(columns=['ID'])
        studentproinfo["TermYear"] = studentproinfo["TermYear"].astype(int64)
        studentproinfo["Semester"] = studentproinfo["Semester"].astype(int64)
        all = pd.concat([enrollment,studentproinfo]).drop_duplicates()
        differ = pd.concat([all,enrollment]).drop_duplicates(keep=False).set_index("StudentID")
        differ.to_sql(
            name="Enrollment", con=self.cursor, if_exists="append"
        )

    def AddStuInfo(self,newinfo):
        newinfo.to_sql(name="Student", con=self.cursor, if_exists="append")
    def UpdateStuInfo(self, stuid, newinfo):

        newinfosql = "Fullname = %s ,Firstname = %s ,CampEmail = %s , HomeEmail = %s , Gender = %s ,Birthday = %s ,Address = %s , Country = %s , ContactInfo = %s , Status ='Y' "
        condition = "WHERE ID = '%s'" % stuid
        sql = "UPDATE Student SET " + newinfosql+condition
        # newinfo =newinfo.fillna('')
        val = (newinfo.iloc[0]['Fullname'],newinfo.iloc[0]['Firstname'],newinfo.iloc[0]['CampEmail'],newinfo.iloc[0]['HomeEmail'],newinfo.iloc[0]['Gender'],newinfo.iloc[0]['Birthday'],newinfo.iloc[0]['Address'],newinfo.iloc[0]['Country'],newinfo.iloc[0]['ContactInfo'])
        self.mycursor.execute(sql,val)
        self.mydb.commit()
