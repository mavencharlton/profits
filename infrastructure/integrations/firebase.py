import firebase_admin
from firebase_admin import credentials, firestore


class FirebaseClient:
    def __init__(self, credential_path: str):
        if not firebase_admin._apps:
            cred = credentials.Certificate(credential_path)
            firebase_admin.initialize_app(cred)
        self.db = firestore.client()

    def save(self, collection: str, doc_id: str, data: dict):
        self.db.collection(collection).document(doc_id).set(data)

    def save_batch(self, collection: str, docs: dict[str, dict]):
        batch = self.db.batch()
        for doc_id, data in docs.items():
            ref = self.db.collection(collection).document(doc_id)
            batch.set(ref, data)
        batch.commit()

    def get(self, collection: str, doc_id: str) -> dict | None:
        doc = self.db.collection(collection).document(doc_id).get()
        return doc.to_dict() if doc.exists else None

    def query(self, collection: str, filters: list[tuple]) -> list[dict]:
        ref = self.db.collection(collection)
        for field, op, value in filters:
            ref = ref.where(field, op, value)
        return [doc.to_dict() for doc in ref.stream()]

    def delete(self, collection: str, doc_id: str):
        self.db.collection(collection).document(doc_id).delete()
