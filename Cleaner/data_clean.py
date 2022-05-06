from distutils.log import warn
import pandas as pd
import glob
from warn import bcolors
class Clean:
    def __init__(self,path):
        print("Loading File From Folder and import into Pandas")
        for file in glob.glob(path):
            file_path = file
        self.initfile = pd.read_excel(file_path)
        self.initfile = self.initfile.drop(columns=['ACADEMIC_LOAD','CHARGES','PYMTS',])


    def CheckDuplicate(self,subset):
        print("check Duplicate Data from files")
        if len(self.initfile[self.initfile.duplicated(subset=subset)])>0:
            print(f"{bcolors.WARNING}Fund {str(len(self.initfile[self.initfile.duplicated(subset=subset)]))} duplicated record(s) !!!{bcolors.ENDC}")
            return True,self.initfile[self.initfile.duplicated(subset=subset,keep=False)]
        else:
            return False,self.initfile


    def CheckNameWithoutComma(self,column):
        print("check Student Name without comma")
        nocomma_name = True
        while nocomma_name:
            nocomma = self.initfile[~self.initfile[column].str.contains(',', na=False)]
            nocomma_name = len(nocomma)>0
            if nocomma_name:
                print(f"{bcolors.WARNING}Fund {str(len(nocomma))} no-comma included record(s) !!!{bcolors.ENDC}")
                print(nocomma)
                # action = int(input("choose action : \n1.Auto Fix (add comma at first space location)\n2.Manully fix\n:"))
                action = 1
                if action == 1:
                    indexlist = nocomma.index
                    for index in indexlist:
                        self.initfile.at[index,column] = self.initfile.at[index,column].replace(' ',',',1)
                    
                else:
                    quit()
                
        return self.initfile
    
    def FormatDf(self) -> pd.DataFrame:
        self.initfile[["Term", "TermYear"]] = self.initfile.TERMDESC.str.split(
            " ",
            expand=True,
        )
        self.initfile[
            ["Program_code", "Semester"]
        ] = self.initfile.ACAD_PROG_PRIMARY.str.split(
            " ",
            expand=True,
        )
        self.initfile[["Firstname", "Lastname"]] = self.initfile.NAME.str.split(
            ",",
            expand=True,
        )
        self.initfile["Semester"] = self.initfile["Semester"].str[-1:]
        self.initfile['EMPLID'] = self.initfile['EMPLID'].apply(str)
        self.initfile["StudentID"] = "W0" + self.initfile["EMPLID"]
        self.initfile['BIRTHDATE'] = self.initfile["BIRTHDATE"].astype(str).str[0:10]
        self.initfile =self.initfile.drop(
            columns=[
                "TERMDESC",
                "ACAD_PROG_PRIMARY",
                "EMPLID",
                "PROG_DESCR",
            ]
        )
        self.initfile = self.initfile.rename(
            columns={
                "CAMPUS": "Campus_code",
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
        return self.initfile