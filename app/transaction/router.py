import datetime
from fastapi import APIRouter, Response
from app.transaction.services import create_index, fetch_amount, ingest_data, retrieve_from_pinecone

from app.transaction.schema import UserQuery
from app.core.custom_renderer import CustomJSONResponse

from langchain_community.chat_models import ChatOpenAI
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate


router = APIRouter()
# llm = ChatOllama(model="llama3", temperature=0)


@router.get('/query', response_class=CustomJSONResponse)
async def query(user_query: UserQuery, response: Response):
    create_index()
    ingest_data()
    user_question = user_query.query
    context = retrieve_from_pinecone(user_question)
    print('context: ', context)
    # amount = fetch_amount(user_question)
    
    template = """
        Answer the question below according to your knowledge in a way that will be helpful to users asking the question. If no answer is found, return "No answer found".
        The following context is your only source of knowledge to answer from.
        user question: {user_question}
        context: {context}
        today_date: {today_date}
    """

    prompt = ChatPromptTemplate.from_template(template)
    llm = ChatOpenAI(model="gpt-4o", temperature=0)

    chain = prompt | llm | StrOutputParser()

    response = ''.join(chain.stream({
        "user_question": user_question,
        "context": context,
        "today_date": datetime.datetime.today().strftime('%Y-%m-%d')
    }))

    return response

