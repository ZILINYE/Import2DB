
import firebase_admin
from firebase_admin import credentials,firestore

cred = credentials.Certificate("my-ace-staff-firebase-adminsdk-ebncq-e75c331dc5.json")
firebase_admin.initialize_app(cred,{'databaseURL':'https://my-ace-staff.firebaseio.com/'})
db = firestore.client()
doc_ref = db.collection(u'Test1')

docs = doc_ref.stream()
for doc in docs:
    print(f'{doc.id} => {doc.to_dict()}')

