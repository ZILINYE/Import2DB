import pandas as pd


def main():
    path = "question.xlsx"
    ori = pd.read_excel(path)
    ori = ori.drop(
        columns=[
            "Timestamp",
            "Email Address",
            "What is your program of study for Spring 2022?",
            "Have you logged into your my St. Clair account (MySIS) and activated your college email? If you encounter any problems during the process, please visit https://www.stclaircollege.ca/it-services/contact for further student account assistance..1",
            "Video Code for Academic and Classroom Policies?",
            "Video Code for of My St. Clair account Guide?",
            "Video Code for Student Services?",
            "Are you interested in deferring your admission until September 2022 if you have not yet received your student visa?",
            "What is your program of study for Spring 2022?.1",
            "If you are planning to travel to Canada, please immediately notify the International Recruitment Office (international@stclaircollege.ca) to confirm his/her intent to travel outside of Canada, as well as to return to Canada.  Please confirm that you have acknowledged on this information.",
            "Have you logged into your my St. Clair account (MySIS) and activated your college email? If you encounter any problems during the process, please visit https://www.stclaircollege.ca/it-services/contact for further student account assistance.",
            "Video Code for Academic and Classroom Policies?.1",
            "Video Code for Student Services?.1",
            "Video Code for of My St. Clair account Guide?.1",

        ]
    )
    ori = ori.drop_duplicates(subset=['Student ID Number (ex: 0712345)'])
    ori['What is your phone number?']=ori['What is your phone number?'].fillna(ori['What is your Whatsapp Number?'])
    ori.to_excel("Output.xlsx")


main()
