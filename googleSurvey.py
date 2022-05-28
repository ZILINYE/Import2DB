from calendar import day_abbr
from re import S
import pandas as pd
import glob

def main():
    pd.set_option('display.max_columns', 500)
    for file in glob.glob("original.xlsx"):
        file_path = file
        data = pd.read_excel(file_path)

        data = data.drop_duplicates(subset=['Student ID Number (ex: 0712345)'])
        # print(data)
        data['phone']=data['phone'].fillna(data['whatsapp'])
        # data.drop(columns=[''])
        print(data)
        data.to_excel('output.xlsx')
        
main()