import datetime
import asyncio
from fastapi import APIRouter, Response, WebSocket, WebSocketDisconnect
from app.transaction.services import create_index, ingest_data, ingest_data_async, retrieve_from_pinecone, to_ascii_safe

from app.transaction.schema import UserQuery
from app.core.custom_renderer import CustomJSONResponse

from langchain_community.chat_models import ChatOpenAI
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate

from langchain_community.embeddings import SentenceTransformerEmbeddings

from app.utils.fetch_data import Database


router = APIRouter()
# llm = ChatOllama(model="llama3", temperature=0)
from pinecone import Pinecone
import os


@router.post('/query', response_class=CustomJSONResponse)
async def query(user_query: UserQuery, response: Response):
    # create_index()
    # ingest_data()
    user_question = user_query.query
    context = retrieve_from_pinecone(user_question)
    
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

    # return chain.stream({
    #     "user_question": user_question,
    #     "context": context,
    #     "today_date": datetime.datetime.today().strftime('%Y-%m-%d')
    # })

    response = "".join(chain.stream({
        "user_question": user_question,
        "context": context,
        "today_date": datetime.datetime.today().strftime('%Y-%m-%d')
    }))
    return response

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
    create_index("all-transactions-v4")
    await ingest_data_async("all-transactions-v4")

    return {"message": "Embeddings generated successfully"}


@router.post('/generate-user-summary-embeddings', response_class=CustomJSONResponse)
async def generate_user_summary_embeddings(response: Response):
    INDEX_NAME = os.environ.get("INDEX_NAME")
    pc = Pinecone(api_key=os.environ.get("PINECONE_API_KEY"))
    index = Pinecone.Index(pc, INDEX_NAME)
    db = Database()

    user_ids = db.get_user_ids()
    for id in user_ids:
        user_id = id[0]

        vector_id = f"{to_ascii_safe(user_id)}_summary"
        response = index.fetch(ids=[vector_id])

        if vector_id in response.vectors:
            print(f"{vector_id} is skipped...")
            continue

        withdraw = db.get_withdraw_amount(user_id)
        topup = db.get_topup_amount(user_id)
        bonus = db.get_bonus(user_id)

        if bonus == "None":
            bonus = "0"

        if withdraw == "None":
            withdraw = "0"
        
        if topup == "None":
            topup = "0"
        
        lifetime_winlose = float(topup.replace(",", "")) - float(withdraw.replace(",", ""))
        affiliate_win_lose = float(topup.replace(",", "")) - float(withdraw.replace(",", "")) - float(bonus.replace(",", ""))
        
        may_win_lose = db.get_may_month_win_lose(user_id)
        april_win_lose = db.get_april_month_win_lose(user_id)
        march_win_lose = db.get_march_month_win_lose(user_id)
        monthly_topup = db.get_monthly_topup(user_id)
        monthly_withdraw = db.get_monthly_withdraw(user_id)
        today_topup = db.get_today_topup(user_id)
        today_withdraw = db.get_today_withdraw(user_id)
        transfer_in = db.get_total_transfer_in(user_id)
        transfer_out = db.get_total_transfer_out(user_id)
        monthly_winlose = float(monthly_topup.replace(",", "")) - float(monthly_withdraw.replace(",", ""))

        embedding_input = f"""
            The summary of user {user_id} is,
            lifetime withdraw amount RM {withdraw},
            lifetime topup amount RM {topup},
            lifetime bonus RM {bonus},
            lifetime win/lose RM {lifetime_winlose},
            lifetime affiliate win/lose RM {affiliate_win_lose},
            May month win/lose RM {may_win_lose},
            April month win/lose RM {april_win_lose},
            March month win/lose RM {march_win_lose},
            Monthly topup amount RM {monthly_topup},
            Monthly withdraw amount RM {monthly_withdraw},
            Monthly win/lose amount RM {monthly_winlose},
            Today topup amount RM {today_topup},
            Today withdraw amount RM {today_withdraw},
            Total transfer-in amount RM {transfer_in},
            Total transfer-out amount RM {transfer_out}.
        """
        model = SentenceTransformerEmbeddings(model_name=os.environ.get("EMBEDDING_MODEL_NAME"))
        embeddings = model.embed_query(embedding_input)

        metadata = {
            "text": embedding_input,
            "user_id": user_id,
            "lifetime_withdraw_amount": withdraw,
            "lifetime_topup_amount": topup,
            "lifetime_bonus_amount": bonus,
            "lifetime_win_or_lose_amount": lifetime_winlose,
            "lifetime_affiliate_win_or_lose_amount": affiliate_win_lose,
            "may_month_win_or_lose_amount": may_win_lose,
            "april_month_win_or_lose_amount": april_win_lose,
            "march_month_win_or_lose_amount": march_win_lose,
            "monthly_topup_amount": monthly_topup,
            "monthly withdraw_amount": monthly_withdraw,
            "monthly_win_or_lose_amount": monthly_winlose,
            "today_topup_amount": today_topup,
            "today_withdraw_amount": today_withdraw,
            "total_transfer_in_amount": transfer_in,
            "total_transfer_out_amount": transfer_out,
        }

        vectors = [
            {"id": vector_id, "values": embeddings, "metadata": metadata}
        ]
        index.upsert(vectors)
        print(f'User {user_id} summary addded...')

    return {"message": "Embedding generated successfully"}


@router.post('/generate-additional-info-embeddings', response_class=CustomJSONResponse)
async def get_additional_info():
    INDEX_NAME = os.environ.get("INDEX_NAME")
    pc = Pinecone(api_key=os.environ.get("PINECONE_API_KEY"))
    index = Pinecone.Index(pc, INDEX_NAME)
    db = Database()

    user_ids = db.get_user_ids()
    for id in user_ids:
        user_id = id[0]

        vip_info_df = db.get_vip_info(user_id)
        vip_info = vip_info_df.to_dict(orient='records')

        playerdet_id = db.get_playerdet_id(user_id)
        # promotion_summary_df = db.get_monthly_promotion_summary(playerdet_id)
        # promotion_summary = promotion_summary_df.to_dict(orient='records')

        bank_account_df = db.get_back_account_info(playerdet_id)
        bank_accounts = bank_account_df.to_dict(orient='records')

        embedding_input = f"""
            VIP reports of user {user_id}:
            {vip_info}
        """
        model = SentenceTransformerEmbeddings(model_name=os.environ.get("EMBEDDING_MODEL_NAME"))
        vip_embeddings = model.embed_query(embedding_input)

        vip_metadata = {
            "text": embedding_input,
            "user_id": user_id,
        }

        # promotion_embedding_input = f"""
        #     Monthly promotion summary of user {user_id}:
        #     {promotion_summary}
        # """
        # model = SentenceTransformerEmbeddings(model_name=os.environ.get("EMBEDDING_MODEL_NAME"))
        # promotion_embeddings = model.embed_query(promotion_embedding_input)

        # promotion_metadata = {
        #     "text": promotion_embedding_input,
        #     "user_id": user_id,
        # }

        bank_embedding_input = f"""
            Bank accounts of user {user_id}:
            {bank_accounts}
        """
        model = SentenceTransformerEmbeddings(model_name=os.environ.get("EMBEDDING_MODEL_NAME"))
        bank_account_embeddings = model.embed_query(bank_embedding_input)

        bank_account_metadata = {
            "text": bank_embedding_input,
            "user_id": user_id,
        }

        vectors = [
            {"id": f"{to_ascii_safe(user_id)}_vip_info", "values": vip_embeddings, "metadata": vip_metadata},
            # {"id": f"{to_ascii_safe(user_id)}_promotion_sammary", "values": promotion_embeddings, "metadata": promotion_metadata},
            {"id": f"{to_ascii_safe(user_id)}_bank_accounts", "values": bank_account_embeddings, "metadata": bank_account_metadata},
        ]
        index.upsert(vectors)
        print(f'User {user_id} information addded...')

    return {"message": "Embedding generated successfully"}


@router.get('/index-info', response_class=CustomJSONResponse)
async def index_info(response: Response):
    # create_index("all-transactions-v2")

    pc = Pinecone(api_key=os.environ.get("PINECONE_API_KEY"))
    index = pc.Index("all-transactions-v4")
    stats = index.describe_index_stats()
    print('stats: ', stats)


    return {"message": "Index info retrieved successfully"}
