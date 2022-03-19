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
                "mysql+pymysql://it:Acumen321@192.168.5.235/Ace", pool_recycle=3600
            )

        except:
            print(f"Error connecting to MariaDB Platform: {e}")
            print("error on connecting to mysql")
            sys.exit(1)

        # Get Cursor
        self.cursor = sqlEngine.connect()

    def GetProgramInfo(self, Pcode):
        condition = ""
        if len(Pcode) > 0:
            condition = "WHERE Program_code='%s'" % Pcode
        sql = "SELECT Program_code,Program_desc FROM ProgramInfo " + condition
        df = pd.read_sql(sql, self.cursor)
        print(df)
        result = df.to_json(orient="records")
        parsed = json.loads(result)
        jsonlist = json.dumps(parsed, indent=4)
        print(jsonlist)
    def GetStudentInfo(self,Sid) -> pd.DataFrame: 
        condition = ""
        if len(Sid) > 0:
            condition = "WHERE ID='%s'" % Sid
        sql = "SELECT * FROM StudentInfo " + condition
        studentinfo = pd.read_sql(sql, self.cursor)
        return studentinfo
    
    def ImportMariadb(self):
        pd.set_option('display.max_columns', 500)
        pd.set_option('display.width', None)
        pd.set_option('display.max_colwidth', None)
        studentondb = self.GetStudentInfo(Sid='')
        self.dataf[["Term", "TermYear"]] = self.dataf.TERMDESC.str.split(
            " ",
            expand=True,
        )
        self.dataf[
            ["Program_code", "Semester"]
        ] = self.dataf.ACAD_PROG_PRIMARY.str.split(
            " ",
            expand=True,
        )
        self.dataf[["Firstname", "Lastname"]] = self.dataf.NAME.str.split(
            ",",
            expand=True,
        )
        self.dataf["Semester"] = self.dataf["Semester"].str[-1:]
        self.dataf['EMPLID'] = self.dataf['EMPLID'].apply(str)
        self.dataf["StudentID"] = "W0" + self.dataf["EMPLID"]
        self.dataf['BIRTHDATE'] = self.dataf["BIRTHDATE"].astype(str).str[0:10]
        self.dataf =self.dataf.drop(
            columns=[
                "TERMDESC",
                "ACAD_PROG_PRIMARY",
                "EMPLID",
                "PROG_DESCR",
            ]
        )
        self.dataf = self.dataf.rename(
            columns={
                "CAMPUS": "CampusCode",
                "Class": "Section",
                "NAME": "Fullname",
                "BIRTHDATE": "Birthday",
                "SEX": "Gender",
                "COUNTRY": "Country",
                "Phone/WhatApps": "ContactInfo",
                "CAMP_EMAIL": "CampEmail",
                "HOME_EMAIL":"HomeEmail",
            }
        )
        studentInfo = self.dataf.drop(columns=['Section','CampusCode','Term','TermYear','Semester','Program_code']).rename(columns={'StudentID':'ID'})
        droplist= studentondb['ID'].tolist()
        differ = pd.concat([studentInfo,studentondb]).drop_duplicates(subset=['ID']).set_index('ID').drop(droplist)
        differ.to_sql(name='StudentInfo',con=self.cursor,if_exists='append')
        # print(studentInfo)
