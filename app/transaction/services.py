import os
import warnings
import json
import sys
from pinecone import Pinecone
from pinecone import ServerlessSpec
from dotenv import load_dotenv

from langchain_pinecone import PineconeVectorStore
from langchain_community.embeddings import SentenceTransformerEmbeddings
from langchain_community.chat_models import ChatOllama
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
# from langchain_text_splitters import RecursiveCharacterTextSplitter

from app.utils.fetch_data import Database
from openai import OpenAI

load_dotenv()

client = OpenAI()
INDEX_NAME = "all-transactions"
pc = Pinecone(api_key=os.environ.get("PINECONE_API_KEY"))


def fetch_amount(user_query):
    # Fetch the amount from the user query
    query_parts = user_query.split()
    amount = None

    for i in range(len(query_parts)):
        part = query_parts[i]
        if part.startswith("RM") or query_parts[i-1] == "RM" or query_parts[i-1].lower() == "amount":
            try:
                amount = int(part[2:])
                break
            except ValueError:
                continue

    return amount

def save_last_trxdt(new_trxdt):
    with open("last_state.json", "w") as f:
        json.dump({"last_trxdt": new_trxdt}, f)


def create_index():
    for index in pc.list_indexes():
        if INDEX_NAME == index.get("name"):
            pc.delete_index(INDEX_NAME)

    pc.create_index(name=INDEX_NAME, dimension=1536, metric="cosine", spec=ServerlessSpec(
        cloud='aws', 
        region='us-east-1'
    ))

    index = pc.Index(INDEX_NAME)


def generate_embeddings(embedding_input):
    response = client.embeddings.create(
        input=embedding_input,
        model=os.environ.get("EMBEDDING_MODEL_NAME"),
    )
    embeddings = response.data[0].embedding
    return embeddings


# def upsert_embeddings(index, ids, embeddings, metadata_list):
#     vectors = [
#         {"id": str(id_), "values": embedding, "metadata": metadata}
#         for id_, embedding, metadata in zip(ids, embeddings, metadata_list)
#     ]
#     index.upsert(vectors)

def upsert_embeddings(index, ids, embeddings, metadata_list, max_batch_size_bytes=4 * 1024 * 1024):
    batch = []
    batch_size = 0

    for id_, embedding, metadata in zip(ids, embeddings, metadata_list):
        vector = {
            "id": str(id_),
            "values": embedding,
            "metadata": metadata
        }
        vector_size = estimate_vector_size(vector)

        # If adding this vector exceeds the batch size limit, upsert the current batch
        if batch and (batch_size + vector_size > max_batch_size_bytes):
            index.upsert(batch)
            batch = []
            batch_size = 0

        batch.append(vector)
        batch_size += vector_size

    # Upsert any remaining vectors
    if batch:
        index.upsert(batch)

# def upsert_embeddings(index, ids, embeddings, metadata_list, batch_size=200):
#     for start in range(0, len(ids), batch_size):
#         end = start + batch_size
#         batch_ids = ids[start:end]
#         batch_embeddings = embeddings[start:end]
#         batch_metadata = metadata_list[start:end]

#         vectors = [
#             {"id": str(id_), "values": embedding, "metadata": metadata}
#             for id_, embedding, metadata in zip(batch_ids, batch_embeddings, batch_metadata)
#         ]
#         index.upsert(vectors)


def generate_embeddings_from_text(row):
    embedding_input = f"User {row["FLNAME"]} made a {row["TRANC_TYPE"]} of RM {row["AMOUNT"]} on {row["TRANCDT"]}."
    embeddings = generate_embeddings(embedding_input)
    return embeddings

def create_metadata(row):
    pass

def estimate_vector_size(vector):
    """
    Estimate the size of a single vector, including its metadata, in bytes.
    """
    return sys.getsizeof(json.dumps(vector).encode('utf-8'))


def ingest_data():
    warnings.filterwarnings('ignore')

    index = pc.Index(INDEX_NAME)
    index.describe_index_stats()

    db = Database()

    for cml_data in db.get_cml_data():
        print(cml_data.head())

        ids = cml_data['TRANC_ID'].tolist()
        metadata_list = cml_data.to_dict(orient='records')

        cml_data['embeddings'] = cml_data.apply(generate_embeddings_from_text, axis=1)
        embeddings = cml_data['embeddings'].tolist()
        # cml_data['metadata'] = cml_data.apply(create_metadata, axis=1)

        # print('ids: ', ids)
        # print('metadata_list: ', metadata_list)
        upsert_embeddings(index, ids, embeddings, metadata_list)

    # cml_data = db.get_cml_data()
    # vectors = []
    # for i in range(0, len(cml_data)):
    #     if i == 0:
    #         last_trxdt = cml_data[i][2]
    #         print('last_trxdt: ', last_trxdt)
    #         save_last_trxdt(last_trxdt)
        
    #     user_id, flname, gender, trans_id, trans_date, type_, amount = cml_data[i]
    #     embedding_input = f"User {flname} made a {type_} of RM {amount} on {trans_date}."
    #     embedding = generate_embeddings(embedding_input)
    #     # embedding = model.embed_query(embedding_input)

    #     vectors.append({
    #         'id': str(trans_id),
    #         'values': embedding,
    #         'metadata': {
    #             'text': embedding_input,
    #             'user_id': user_id,
    #             'user_name': flname,
    #             'gender': gender,
    #             'trans_date': str(trans_date),
    #             'type': type_,
    #             'amount': amount,
    #         }
    #     })

    # index.upsert(vectors=vectors)


def retrieve_from_pinecone(user_query):
    model = SentenceTransformerEmbeddings(model_name=os.environ.get("EMBEDDING_MODEL_NAME"))
    pinecone = PineconeVectorStore.from_existing_index(index_name=INDEX_NAME, embedding=model)
    context = pinecone.similarity_search(user_query)
    return context

