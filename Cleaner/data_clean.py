from distutils.log import warn
import imp
import string
import pandas as pd
import glob
from warn import bcolors
# import os.path
class Clean:
    def __init__(self,path):
        print("Loading File From Folder and import into Pandas")
        for file in glob.glob(path):
            file_path = file
        self.initfile = pd.read_excel(file_path)
        self.initfile = self.initfile.drop(columns=['BIRTHDATE','SEX','ACADEMIC_LOAD','COUNTRY','Phone/WhatApps','Address','HOME_EMAIL','CHARGES','PYMTS','Outstanding Fee','Comment'])


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
                action = int(input("choose action : \n1.Auto Fix (add comma at first space location)\n2.Manully fix\n:"))
                if action == 1:
                    indexlist = nocomma.index
                    for index in indexlist:
                        self.initfile.at[index,column] = self.initfile.at[index,column].replace(' ',',',1)
                    
                else:
                    quit()
                
        return nocomma_name,self.initfile
                