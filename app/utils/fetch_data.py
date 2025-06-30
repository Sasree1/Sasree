import oracledb
import os
import asyncio
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

ORACLE_CONFIG = {
    "user": os.getenv("ORACLE_USER"),
    "password": os.getenv("ORACLE_PASSWORD"),
    "dsn": os.getenv("ORACLE_DSN"),
    "mode": oracledb.AuthMode.DEFAULT
}

# oracledb.init_oracle_client(lib_dir=r"F:\OracleDB\instantclient_23_8")

class Database:
    def __init__(self):
        self.config = ORACLE_CONFIG
        self.connection = None
    
    def connect(self):
        if self.connection is None:
            self.connection = oracledb.connect(**self.config)
        return self.connection
    
    def get_user_ids(self):
        conn = self.connect()
        cursor = conn.cursor()

        query = """
            select DISTINCT user_id from
            (
            select user_id from wev_tranc
            union all
            select user_id from wet_pwl
            )
        """
        cursor.execute(query)
        records = cursor.fetchall()
        return records
    
    def get_cml_data_sync(self):
        conn = self.connect()
        # cursor = conn.cursor()

        # query = """SELECT * FROM wet_topup WHERE rownum <= 10"""
        query = open("app/utils/queries/CML-page-transaction-v4.sql", "r").read()
        # query = open("app/utils/queries/CML-page-transaction-v2.sql", "r").read()

        # cursor.execute(query, P63_USER_ID="HPWIN1015")
        # cursor.execute(query)
        # records = cursor.fetchall()
        
        # cursor.close()
        # return records

        data = pd.read_sql(query, con=conn)
        return data
    

    def get_withdraw_amount(self, user_id):
        conn = self.connect()
        cursor = conn.cursor()

        query = """
            select to_char(nvl(sum(withdraw), 0), '999G999G999G999G990D00') from 
            (
            select sum(abs(withdraw)) withdraw from wev_tranc
            where user_id=:P63_USER_ID
            union all
            select sum(abs(withdraw)) withdraw from wet_pwl
            where user_id=:P63_USER_ID
            )
        """
        cursor.execute(query, P63_USER_ID=user_id)
        records = cursor.fetchone()
        return str(records[0]).strip()
    
    def get_topup_amount(self, user_id):
        conn = self.connect()
        cursor = conn.cursor()

        query = """
            select to_char(nvl(sum(top_up), 0), '999G999G999G999G990D00') from
            (
            select sum(top_up) top_up from wev_tranc
            where user_id=:P63_USER_ID
            union all
            select sum(top_up) top_up from wet_pwl
            where user_id=:P63_USER_ID
            )
        """
        cursor.execute(query, P63_USER_ID=user_id)
        records = cursor.fetchone()
        return str(records[0]).strip()
    
    def get_bonus(self, user_id):
        conn = self.connect()
        cursor = conn.cursor()

        query = """
            select to_char(sum(nvl(bonus, 0)), '999G999G999G999G990D00')
            from wet_topup
            where user_id=:P63_USER_ID
            and status = 'P'
        """
        cursor.execute(query, P63_USER_ID=user_id)
        records = cursor.fetchone()
        return str(records[0]).strip()
    
    def get_may_month_win_lose(self, user_id):
        conn = self.connect()
        cursor = conn.cursor()

        query = """
            select to_char(nvl(sum(top_up), 0), '999G999G999G999G990D00') from 
            (
            select sum(nvl(top_up, 0)) top_up from wev_tranc
            where user_id=:P63_USER_ID
            and type='Top Up'
            and to_char(trxdt,'YYYYMM')=to_char(add_months(trunc(sysdate,'mm'),-1) ,'YYYYMM')

            union all

            select sum(nvl(withdraw, 0)) * -1 topup from wev_tranc
            where user_id=:P63_USER_ID
            and to_char(trxdt,'YYYYMM')=to_char(add_months(trunc(sysdate,'mm'),-1) ,'YYYYMM')
            and type='Withdraw'
            )
        """
        cursor.execute(query, P63_USER_ID=user_id)
        records = cursor.fetchone()
        return str(records[0]).strip()
    
    def get_april_month_win_lose(self, user_id):
        conn = self.connect()
        cursor = conn.cursor()

        query = """
            select to_char(nvl(sum(top_up), 0), '999G999G999G999G990D00') from 
            (
            select sum(nvl(top_up, 0)) top_up from wev_tranc
            where user_id=:P63_USER_ID
            and type='Top Up'
            and to_char(trxdt,'YYYYMM')=to_char(add_months(trunc(sysdate,'mm'),-2) ,'YYYYMM')

            union all

            select sum(nvl(withdraw, 0)) * -1 topup from wev_tranc
            where user_id=:P63_USER_ID
            and to_char(trxdt,'YYYYMM')=to_char(add_months(trunc(sysdate,'mm'),-2) ,'YYYYMM')
            and type='Withdraw'
            )
        """
        cursor.execute(query, P63_USER_ID=user_id)
        records = cursor.fetchone()
        return str(records[0]).strip()
    
    def get_march_month_win_lose(self, user_id):
        conn = self.connect()
        cursor = conn.cursor()

        query = """
            select to_char(nvl(sum(top_up), 0), '999G999G999G999G990D00') from 
            (
            select sum(nvl(top_up, 0)) top_up from wev_tranc
            where user_id=:P63_USER_ID
            and type='Top Up'
            and to_char(trxdt,'YYYYMM')=to_char(add_months(trunc(sysdate,'mm'),-3) ,'YYYYMM')

            union all

            select sum(nvl(withdraw, 0)) * -1 topup from wev_tranc
            where user_id=:P63_USER_ID
            and to_char(trxdt,'YYYYMM')=to_char(add_months(trunc(sysdate,'mm'),-3) ,'YYYYMM')
            and type='Withdraw'
            )
        """
        cursor.execute(query, P63_USER_ID=user_id)
        records = cursor.fetchone()
        return str(records[0]).strip()

    def get_monthly_topup(self, user_id):
        conn = self.connect()
        cursor = conn.cursor()

        query = """
            select to_char(nvl(sum(top_up),0), '999G999G999G999G990D00') from 
            (
            select sum(top_up) top_up from wev_tranc
            where user_id=:P63_USER_ID
            and type='Top Up'
            and to_char(trxdt,'YYYYMM')=to_char(sysdate,'YYYYMM'))
        """
        cursor.execute(query, P63_USER_ID=user_id)
        records = cursor.fetchone()
        return str(records[0]).strip()
    
    def get_monthly_withdraw(self, user_id):
        conn = self.connect()
        cursor = conn.cursor()

        query = """
            select to_char(nvl(sum(withdraw),0), '999G999G999G999G990D00') from 
            (
            select sum(abs(withdraw)) withdraw from wev_tranc
            where user_id=:P63_USER_ID
            and to_char(trxdt,'YYYYMM')=to_char(sysdate,'YYYYMM')
            and type='Withdraw'
            )
        """
        cursor.execute(query, P63_USER_ID=user_id)
        records = cursor.fetchone()
        return str(records[0]).strip()
    
    def get_today_topup(self, user_id):
        conn = self.connect()
        cursor = conn.cursor()

        query = """
            select to_char(nvl(top_up, 0), '999G999G999G999G990D00') from 
            (
            select sum(top_up) top_up from wev_tranc
            where user_id=:P63_USER_ID
            and type='Top Up'
            and trunc(trxdt)=trunc(sysdate))
        """
        cursor.execute(query, P63_USER_ID=user_id)
        records = cursor.fetchone()
        return str(records[0]).strip()
    
    def get_today_withdraw(self, user_id):
        conn = self.connect()
        cursor = conn.cursor()

        query = """
            select to_char(nvl(sum(withdraw),0), '999G999G999G999G990D00') from 
            (
            select sum(abs(withdraw)) withdraw from wev_tranc
            where user_id=:P63_USER_ID
            and trunc(trxdt)=trunc(sysdate)
            and type='Withdraw'
            )
        """
        cursor.execute(query, P63_USER_ID=user_id)
        records = cursor.fetchone()
        return str(records[0]).strip()
    
    def get_total_transfer_in(self, user_id):
        conn = self.connect()
        cursor = conn.cursor()

        query = """
            select to_char(nvl(sum(amount),0), '999G999G999G999G990D00')
            from wet_tp
            where to_user=:P63_USER_ID
            and status='P'
        """
        cursor.execute(query, P63_USER_ID=user_id)
        records = cursor.fetchone()
        return str(records[0]).strip()
    
    def get_total_transfer_out(self, user_id):
        conn = self.connect()
        cursor = conn.cursor()

        query = """
            select to_char(nvl(sum(amount),0) , '999G999G999G999G990D00')
            from wet_tp
            where frm_user=:P63_USER_ID
            and status='P'
        """
        cursor.execute(query, P63_USER_ID=user_id)
        records = cursor.fetchone()
        return str(records[0]).strip()
    
    def get_vip_info(self, user_id):
        conn = self.connect()

        query = f"""
            select lvl.name, vip.assigned_at, vip.expires_at
            from hpw.vips vip
            join hpw.vip_levels lvl using (vip_level_id)
            where vip.player_id = '{user_id}'
            order by vip.assigned_at desc
        """
        data = pd.read_sql(query, con=conn)
        return data
    
    def get_playerdet_id(self, user_id):
        conn = self.connect()
        cursor = conn.cursor()

        query = """
            select max(id) from wem_playerdet
            where user_id=:P63_USER_ID
        """

        cursor.execute(query, P63_USER_ID=user_id)
        records = cursor.fetchone()
        return str(records[0]).strip()
    
    def get_monthly_promotion_summary(self, playerdet_id):
        conn = self.connect()
        cursor = conn.cursor()

        query = """
            select a.promotioncd, a.fldesc, nvl(to_char(a.monthly_qty),'Unlimited') monthly_qty ,
            (select count(*) from wet_topup where user_id in (select user_id from wem_playerdet where id=:P63_ID) and status='P' and promotioncd=a.promotioncd and to_char(claimdt,'YYYYMM')=to_char(sysdate,'YYYYMM')) monthly_taken,
            (select count(*) from wet_topup where user_id in (select user_id from wem_playerdet where id=:P63_ID) and status='P' and promotioncd=a.promotioncd) all_time
            from wem_promotion a
            where a.close='N'
            and to_char(sysdate,'YYYYMM')>=to_char(a.eftdtfrm,'YYYYMM')
            and to_char(sysdate,'YYYYMM')<=to_char(a.eftdtto,'YYYYMM')
            order by a.seqno
        """
        cursor.execute(query, P63_ID=playerdet_id)
        records = cursor.fetchall()
        return records

    def get_back_account_info(self, playerdet_id):
        conn = self.connect()
        cursor = conn.cursor()

        query = """
            select bankcd bank_nm, bankcd, bankacc, upper(holder_name) holder_name, user_id, count(*) from wet_withdraw
            where user_id in (select x.user_id from wem_playerdet x where id=:P63_ID)
            and bankacc is not null
            group by bankcd, bankacc, upper(holder_name), user_id
            having count(*)>3
        """
        cursor.execute(query, P63_ID=playerdet_id)
        records = cursor.fetchall()
        return records
    
    async def get_cml_data_async(self):
        def generate():
            chunk_size = 100
            conn = self.connect()
            # cursor = conn.cursor()

            # query = """SELECT * FROM wet_topup WHERE rownum <= 10"""
            # query = open("app/utils/queries/CML-page-transaction.sql", "r").read()
            query = open("app/utils/queries/CML-page-transaction-v4.sql", "r").read()

            # cursor.execute(query, P63_USER_ID="HPWIN1015")
            # cursor.execute(query)
            chunks = pd.read_sql(query, con=conn, chunksize=chunk_size)
            for chunk in chunks:
                yield chunk
        
        loop = asyncio.get_running_loop()
        for chunk in await loop.run_in_executor(None, lambda: list(generate())):
            yield chunk


    def close(self):
        if self.connection:
            self.connection.close()
            self.connection = None

def connect_to_oracle():
    return oracledb.connect(**ORACLE_CONFIG)

