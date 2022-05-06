<<<<<<< HEAD
# from msilib.schema import Error
from audioop import add
from re import U
from unittest.loader import VALID_MODULE_NAME
=======
>>>>>>> cec23b9d00f613bfbdbdbde699380043c91d4be2
from sqlalchemy import create_engine
import sys
import pandas as pd
import json
<<<<<<< HEAD
import difflib
import firebase_admin
from firebase_admin import credentials,firestore
=======
>>>>>>> cec23b9d00f613bfbdbdbde699380043c91d4be2


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

    def GetStudentInfo(self, Sid) -> pd.DataFrame:
        condition = ""
        if len(Sid) > 0:
            condition = "WHERE ID='%s'" % Sid
        sql = "SELECT * FROM StudentInfo " + condition
        studentinfo = pd.read_sql(sql, self.cursor)
        return studentinfo

    def ImportStudentInfo(self):

        # import student info function
        studentondb = self.GetStudentInfo(Sid="")
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
        droplist = studentondb["ID"].tolist()
        differ = (
            pd.concat([studentInfo, studentondb])
            .drop_duplicates(subset=["ID"])
            .set_index("ID")
            .drop(droplist)
        )
<<<<<<< HEAD
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


=======
        differ.to_sql(name="StudentInfo", con=self.cursor, if_exists="append")
>>>>>>> cec23b9d00f613bfbdbdbde699380043c91d4be2
        # print(studentInfo)

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
