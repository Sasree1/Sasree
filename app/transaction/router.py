import datetime
from fastapi import APIRouter, Response, WebSocket, WebSocketDisconnect
from app.transaction.services import create_index, ingest_data, retrieve_from_pinecone

from app.transaction.schema import UserQuery
from app.core.custom_renderer import CustomJSONResponse

from langchain_community.chat_models import ChatOpenAI
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate


router = APIRouter()
# llm = ChatOllama(model="llama3", temperature=0)


@router.get('/query', response_class=CustomJSONResponse)
async def query(user_query: UserQuery, response: Response):
    # create_index()
    # ingest_data()
    user_question = user_query.query
    context = await retrieve_from_pinecone(user_question)
    
    template = """
        Answer the question below according to your knowledge in a way that will be helpful to users asking the question. If no answer is found, return "No answer found".
        The following context is your only source of knowledge to answer from and keep the answer concise and short.
        user question: {user_question}
        context: {context}
        today_date: {today_date}
    """

    prompt = ChatPromptTemplate.from_template(template)
    llm = ChatOpenAI(model="gpt-4o", temperature=0)

    chain = prompt | llm | StrOutputParser()

    return chain.stream({
        "user_question": user_question,
        "context": context,
        "today_date": datetime.datetime.today().strftime('%Y-%m-%d')
    })

@router.websocket("/ws/query")
async def websocket_query(websocket: WebSocket):
    await websocket.accept()

    try:
        while True:
            data = await websocket.receive_json()
            print('data: ', data)
            user_question = data.get("query")

            if not user_question:
                await websocket.send_text("Invalid query. Please send a 'query' field.")
                await websocket.close()

            context = await retrieve_from_pinecone(user_question)

            template = """
                Answer the question below according to your knowledge in a way that will be helpful to users asking the question. If no answer is found, return "No answer found".
                The following context is your only source of knowledge to answer from and keep the answer concise and short.
                user question: {user_question}
                context: {context}
                today_date: {today_date}
            """

            prompt = ChatPromptTemplate.from_template(template)
            llm = ChatOpenAI(model="gpt-4o", temperature=0)
            chain = prompt | llm | StrOutputParser()

            async for chunk in chain.astream({
                "user_question": user_question,
                "context": context,
                "today_date": datetime.datetime.today().strftime('%Y-%m-%d')
            }):
                await websocket.send_text(chunk)

            await websocket.send_text("[[END]]")

    except WebSocketDisconnect:
        print("WebSocket disconnected")

    except Exception as e:
        await websocket.send_text(f"Error: {str(e)}")
        await websocket.close()


@router.get('/generate-embeddings', response_class=CustomJSONResponse)
async def generate_embeddings(response: Response):
    create_index("all-transactions-v2")
    await ingest_data()

    return {"message": "Embeddings generated successfully"}

