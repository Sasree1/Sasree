select * from (
       select user_id, tranc_id, to_char(claimdt,'DD-MON-YYYY HH24:MI:SS') trancdt, 'Top Up' tranc_type, amount from wet_topup
       where claim='Y'
       and status='P'
       and adjustment='N'
       and user_id=:P63_USER_ID

       union all

       select user_id, tranc_id, to_char(issueddt,'DD-MON-YYYY HH24:MI:SS') trancdt, 'Withdraw' tranc_type, (amount*-1) amount from wet_withdraw
       where status='P'
       and user_id=:P63_USER_ID

       union all

       select frm_user user_id, tranc_id, to_char(issueddt,'DD-MON-YYYY HH24:MI:SS') trancdt, 'Transfer Out ('||to_user||')' tranc_type, (amount*-1) amount from wet_tp
       where frm_user=:P63_USER_ID
       and status='P'

       union all

       select to_user user_id, tranc_id, to_char(issueddt,'DD-MON-YYYY HH24:MI:SS') trancdt, 'Transfer In ('||frm_user||')' tranc_type, (amount) amount from wet_tp
       where to_user=:P63_USER_ID
       and status='P'

       union all

       select user_id, tranc_id, db_utfield(cs_approveby,'~',2) trancdt, 'Adjustment' tranc_type, 0 amount from wet_topup
       where user_id=:P63_USER_ID
       and claim='Y'
       and adjustment='Y'
       and nvl(adjustment_type,'U')='U'
       and status='P'

       Union all

       select user_id, tranc_id, to_char(claimdt,'DD-MON-YYYY HH24:MI:SS') trancdt, 'Advance' tranc_type, 0 amount from wet_topup
       where user_id=:P63_USER_ID
       and claim='Y'
       and adjustment='Y'
       and nvl(adjustment_type,'U')='A'
       and status='P'

       Union all

       select user_id, tranc_id, db_utfield(cs_approveby,'~',2) trancdt, 'Referer Bonus' tranc_type, 0 amount from wet_topup
       where user_id=:P63_USER_ID
       and adjustment='Y'
       and nvl(adjustment_type,'U')='R'
       and status='P'

       Union all

       select user_id, null tranc_id, to_char(trxdt,'DD-MON-YYYY HH24:MI:SS') trancdt, 'WL before Admin' tranc_type, wl amount from wet_pwl
       where user_id=:P63_USER_ID
       
) order by trancdt desc
