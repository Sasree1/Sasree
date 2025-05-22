import os
import asyncio
from pinecone import Pinecone
from pinecone import ServerlessSpec
from langchain_community.embeddings import SentenceTransformerEmbeddings
from app.utils.fetch_data import Database

from dotenv import load_dotenv
load_dotenv()

INDEX_NAME = os.environ.get("INDEX_NAME")
pc = Pinecone(api_key=os.environ.get("PINECONE_API_KEY"))

def create_index(INDEX_NAME=INDEX_NAME):
    is_axist = False
    for index in pc.list_indexes():
        if INDEX_NAME == index.get("name"):
            is_axist = True
            break

    if not is_axist:
        pc.create_index(name=INDEX_NAME, dimension=384, metric="cosine", spec=ServerlessSpec(
            cloud='aws', 
            region='us-east-1'
        ))

def upsert_embeddings(index, ids, embeddings, metadata_list):
    vectors = [
        {"id": str(id_), "values": embedding, "metadata": metadata}
        for id_, embedding, metadata in zip(ids, embeddings, metadata_list)
    ]
    print('vectors: ', vectors)
    index.upsert(vectors)


def generate_embeddings(embedding_input):
    model = SentenceTransformerEmbeddings(model_name=os.environ.get("EMBEDDING_MODEL_NAME"))
    embeddings = model.embed_query(embedding_input)
    return embeddings


def generate_embeddings_from_text(row):
    embedding_input = f"User {row["USER_ID"]} made a {row["TRANC_TYPE"]} of RM {row["AMOUNT"]} on {row["TRANCDT"]}."
    embeddings = generate_embeddings(embedding_input)
    return embeddings


def create_metadata(row):
    metadata = {
        "text": f"User {row["USER_ID"]} made a {row["TRANC_TYPE"]} of RM {row["AMOUNT"]} on {row["TRANCDT"]}.",
        "user_id": row["USER_ID"],
        "user_name": row["FLNAME"],
        "transaction_type": row["TRANC_TYPE"],
        'transaction_date': str(row["TRANCDT"]),
        "transaction_amount": row["AMOUNT"],
    }
    return metadata


async def ingest_data_async(INDEX_NAME=INDEX_NAME):
    index = pc.Index(INDEX_NAME)
    db = Database()

    async for cml_data in db.get_cml_data_async():
        print('cml_data_count: ', len(cml_data))
        ids = cml_data['TRANC_ID'].tolist()
        # metadata_list = cml_data.to_dict(orient='records')
        
        cml_data['embeddings'] = cml_data.apply(generate_embeddings_from_text, axis=1)
        cml_data["metadata"] = cml_data.apply(create_metadata, axis=1)
        embeddings = cml_data['embeddings'].tolist()
        metadata_list = cml_data['metadata'].tolist()

        upsert_embeddings(index, ids, embeddings, metadata_list)


if __name__ == "__main__":
    create_index("all-transactions-v3")
    asyncio.run(ingest_data_async(INDEX_NAME="all-transactions-v3"))



