from fastapi import APIRouter, Response
from app.transaction.services import create_index, ingest_data, retrieve_from_pinecone

from app.transaction.schema import UserQuery
from app.core.custom_renderer import CustomJSONResponse

from langchain_community.chat_models import ChatOllama
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate


router = APIRouter()
llm = ChatOllama(model="llama3", temperature=0)


@router.get('/query', response_class=CustomJSONResponse)
async def query(user_query: UserQuery, response: Response):
    create_index()
    ingest_data()
    context = retrieve_from_pinecone(user_query.query)
    print('context: ', context)
    
    template = f"""
        Answer the question below according to your knowledge in a way that will be helpful to users asking the question.
        The following context is your only source of knowledge to answer from.
        Context: {context}
        User question: {user_query.query}
    """
    prompt = ChatPromptTemplate.from_template(template)
    print('prompt: ', prompt)

    chain = prompt | llm | StrOutputParser()

    return chain.stream({
        "context": context,
        "user_question": user_query.query
    })



