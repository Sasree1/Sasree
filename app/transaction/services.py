import os
import warnings
from pinecone import Pinecone
from pinecone import ServerlessSpec
from dotenv import load_dotenv

# from langchain.chains import LLMChain
# from langchain.prompts import PromptTemplate
# from langchain.llms import OpenAI

from langchain_pinecone import PineconeVectorStore
from langchain_community.embeddings import SentenceTransformerEmbeddings
from langchain_community.chat_models import ChatOllama
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser

from app.utils.fetch_data import Database

load_dotenv()

INDEX_NAME = "test"
pc = Pinecone(api_key=os.environ.get("PINECONE_API_KEY"))
llm = ChatOllama(model="llama3", temperature=0)

# from langchain import OpenAI, SQLDatabase, SQLDatabaseChain
# from langchain.embeddings import OpenAIEmbeddings
# from langchain.vectorstores import Pinecone


# def interpret_query(user_input, schema):
#     """
#     Interpret the user's query and identify the entities involved.
#     """
#     # Define a prompt template for interpreting the query
#     prompt = PromptTemplate(
#         input_variables=["user_input", "schema"],
#         template=(
#             "You are an AI assistant. Based on the given schema: {schema}, "
#             "interpret the following user query: {user_input}. "
#             "Identify the entities involved and return them in JSON format."
#         )
#     )

#     llm = OpenAI(model="text-davinci-003", temperature=0)

#     # Create a chain to process the query
#     chain = LLMChain(llm=llm, prompt=prompt)

#     # Run the chain with the user input and schema
#     result = chain.run(user_input=user_input, schema=schema)

#     return result


def create_index():
    for index in pc.list_indexes():
        if INDEX_NAME == index.get("name"):
            print('INDEX_NAME: ', INDEX_NAME)
            pc.delete_index(INDEX_NAME)

    pc.create_index(name=INDEX_NAME, dimension=384, metric="cosine", spec=ServerlessSpec(
        cloud='aws', 
        region='us-east-1'
    ))

    index = pc.Index(INDEX_NAME)
    print("Index:", index.describe_index_stats())


def ingest_data():
    warnings.filterwarnings('ignore')

    index = pc.Index(INDEX_NAME)
    index.describe_index_stats()

    db = Database()
    cml_data = db.get_cml_data()

    model = SentenceTransformerEmbeddings(model_name=os.environ.get("PINECONE_MODEL_NAME"))

    vectors = []
    for row in cml_data:
        user_id, trans_id, trans_date, type_, amount, bonus, rollover, date, category = row
        embedding_input = f"User {user_id} made a {type_} of RM {amount} with bonus {bonus} and rollover {rollover} on {trans_date}."
        embedding = model.embed_query(embedding_input)

        vectors.append({
            'id': str(trans_id),
            'values': embedding,
            'metadata': {
                'text': embedding_input,
                'user_id': user_id,
                'trans_date': str(trans_date),
                'type': type_,
                'amount': amount,
                'bonus': bonus,
                'rollover': rollover,
                'date': str(date),
                'category': category if category else ""
            }
        })

    index.upsert(vectors=vectors)


def retrieve_from_pinecone(user_query):
    model = SentenceTransformerEmbeddings(model_name=os.environ.get("PINECONE_MODEL_NAME"))
    pinecone = PineconeVectorStore.from_existing_index(index_name=INDEX_NAME, embedding=model)
    context = pinecone.similarity_search(user_query)
    
    return context


def get_response(user_query):
    context = retrieve_from_pinecone(user_query)
    print('context: ', context)
    
    template = """
        Answer the question below according to your knowledge in a way that will be helpful to users asking the question.
        The following context is your only source of knowledge to answer from.
        Context: {context}
        User question: {user_query}
        Amount: {amount}
    """
    prompt = ChatPromptTemplate.from_template(template)
    print('prompt: ', prompt)

    chain = prompt | llm | StrOutputParser()
    response = chain.stream({
        "context": context,
        "user_question": user_query
    })
    print('response: ', type(response).__name__)
    
    return ""


