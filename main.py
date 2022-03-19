from Cleaner.data_clean import Clean
from DataProcess.tomaria import Maria


def main():
    print("Start from Begin")
    initData = Clean(path='Files/Spring/Enrolled*.xlsx')
    duplicated, record = initData.CheckDuplicate(subset='EMPLID')
    if duplicated:
        print(record)
        print('Quitting the program')
        quit()
    nocomma, record = initData.CheckNameWithoutComma(column='NAME')
    db = Maria(record)
    # db.GetProgramInfo(Pcode='M018')
    db.ImportMariadb()



main()
