import mysql.connector

mydb = mysql.connector.connect(
  host="192.168.5.238",
  user="it",
  password="Acumen321",
  database="Ace"
)

mycursor = mydb.cursor()

mycursor.execute("DELETE FROM InstructorInfo WHERE ID = 1")

# mycursor.commit()
mydb.commit()
# print(myresult[1])s
# for x in myresult:
#   print(x)