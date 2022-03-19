from Cleaner.data_clean import Clean
from DataProcess.tomaria import Maria


def main():
    
    folderlist = ['Winter','Spring','Fall']
    for folder in folderlist:
        print(f"Working on {folder} folder")
        path= "Files/'%s'/Enrolled*.xlsx" % folder
        initData = Clean(path=path)
        # Checking the Duplicate data for the original excel file on column 'EMPLID'
        duplicated, record = initData.CheckDuplicate(subset='EMPLID')
        if duplicated:
            print(record)
            print('Quitting the program')
            quit()
        # Checking the wrong format Fullname which not contains a comma (used by separate First and last name)
        record = initData.CheckNameWithoutComma(column='NAME')
        # Format the cleaned dataframe to mathch the database title name
        formatted = initData.FormatDf()
        # Initialize the Dataframe data for the next step
        db = Maria(formatted)
        # Import student information into the table 'StudentInfo'
        db.ImportStudentInfo()
        # Import student program register information into the table 'StudentProgram'
        db.ImportStudentProgramInfo()

        
        # Get specfic program student list
        # db.GetProgramInfo(Pcode='M018')


main()
