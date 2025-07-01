import os
from pinecone import Pinecone

from langchain_community.embeddings import SentenceTransformerEmbeddings
from app.utils.fetch_data import Database

from app.transaction.services import to_ascii_safe


def generate_user_summary_embedding():
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


def generate_user_additional_embedding():
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
