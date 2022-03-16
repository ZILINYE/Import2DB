import warnings
from Cleaner.data_clean import Clean
def main():
    print("Start from Begin")
    initData = Clean(path='Files/Fall/Enrolled*.xlsx')
    duplicated,record = initData.CheckDuplicate(subset='EMPLID')
    if duplicated:
        print(record)
        print('Quitting the program')
        quit()
    nocomma,record = initData.CheckNameWithoutComma(column='NAME')
    print(record)

main()