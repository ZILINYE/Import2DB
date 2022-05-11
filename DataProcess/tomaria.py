<<<<<<< HEAD
# from curses.ascii import SI
from tkinter.tix import Tree
from numpy import empty
from sqlalchemy import create_engine, false, true
=======
# from msilib.schema import Error
from audioop import add
from re import U
from unittest.loader import VALID_MODULE_NAME
from sqlalchemy import create_engine
>>>>>>> 001fcb5a275e19a17eab913518ceaf365d7ccbdc
import sys
import pandas as pd
import json
import difflib
import firebase_admin
from firebase_admin import credentials,firestore


class Maria:
    def __init__(self, dataf):
        self.dataf = dataf
        try:
            sqlEngine = create_engine(
<<<<<<< HEAD
                "mysql+pymysql://it:Acumen321@192.168.5.238/Ace", pool_recycle=3600
=======
                "mysql+pymysql://it:Acumen321@192.168.5.235/Ace", pool_recycle=3600
>>>>>>> 001fcb5a275e19a17eab913518ceaf365d7ccbdc
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
<<<<<<< HEAD
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
=======
        df = pd.read_sql(sql, self.cursor)
        print(df)
        result = df.to_json(orient="records")
        parsed = json.loads(result)
        jsonlist = json.dumps(parsed, indent=4)
        print(jsonlist)
    def GetStudentInfo(self,Sid) -> pd.DataFrame: 
        condition = ""
>>>>>>> 001fcb5a275e19a17eab913518ceaf365d7ccbdc
        if len(Sid) > 0:
            condition = "WHERE ID='%s'" % Sid
        sql = "SELECT "+field+" FROM StudentInfo " + condition
        studentinfo = pd.read_sql(sql, self.cursor)
        return studentinfo
<<<<<<< HEAD

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
=======
    
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
>>>>>>> 001fcb5a275e19a17eab913518ceaf365d7ccbdc
            columns=[
                "TERMDESC",
                "ACAD_PROG_PRIMARY",
                "EMPLID",
                "PROG_DESCR",
            ]
<<<<<<< HEAD
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
 
=======
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
       
        # differ = pd.concat([studentInfo,studentondb]).drop_duplicates(subset=['ID']).set_index('ID').drop(droplist)
        # differ.to_sql(name='StudentInfo',con=self.cursor,if_exists='append')


        cred = credentials.Certificate("my-ace-staff-firebase-adminsdk-ebncq-e75c331dc5.json")
        firebase_admin.initialize_app(cred,{'databaseURL':'https://my-ace-staff.firebaseio.com/'})
        db = firestore.client()
        doc_ref = db.collection(u'StudentInfo')

        
        tmp = studentInfo.to_dict(orient='records')
        print(tmp)
        for value in tmp:

            doc_ref.document(value['ID']).set(value)
      
        # list(map(lambda x: doc_ref.add(x), tmp))

        # doc_ref.set(tmp)
        # docs = doc_ref.stream()
        # for doc in docs:
        #     print(f'{doc.id} => {doc.to_dict()}')


        # print(studentInfo)
>>>>>>> 001fcb5a275e19a17eab913518ceaf365d7ccbdc
