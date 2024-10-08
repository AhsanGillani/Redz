from django.conf import settings
import logging

def get_next_document_id():
    db = settings.FIRESTORE_DB
    counter_ref = db.collection('counters').document('Attendance')
    counter_doc = counter_ref.get()

    if counter_doc.exists:
        latest_id = counter_doc.to_dict().get('latest_id', 0)
        new_id = latest_id + 1
        counter_ref.update({'latest_id': new_id})
        return new_id
    else:
        counter_ref.set({'latest_id': 1})
        return 1

def insert_data_batch_to_firestore(collection_name, data_list):
    try:
        db = settings.FIRESTORE_DB
        batch = db.batch()
        for data in data_list:
            new_doc_id = get_next_document_id()
            doc_ref = db.collection(collection_name).document(str(new_doc_id))
            batch.set(doc_ref, data)
        batch.commit()
        logging.info(f"Successfully inserted {len(data_list)} records in batch")
        return True
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return False
