select * from (
       select user_id, tranc_id, to_char(claimdt,'DD-MON-YYYY HH24:MI:SS') trancdt, 'Top Up' tranc_type, amount, bonus, (nvl(amount,0)+nvl(bonus,0)) rollover, claimdt trxdt, '' category from wet_topup
       where claim='Y'
       and status='P'
       and adjustment='N'

       union all

       select user_id, tranc_id, to_char(issueddt,'DD-MON-YYYY HH24:MI:SS') trancdt, 'Withdraw' tranc_type, (amount*-1) amount, 0 bonus, 0 rollover, issueddt trxdt, '' category from wet_withdraw
       where status='P'

       union all

       select frm_user user_id, tranc_id, to_char(issueddt,'DD-MON-YYYY HH24:MI:SS') trancdt, 'Transfer Out ('||to_user||')' tranc_type, (amount*-1) amount, 0 bonus, 0 rollover, issueddt trxdt, '' category from wet_tp
       where status='P'

       union all

       select to_user user_id, tranc_id, to_char(issueddt,'DD-MON-YYYY HH24:MI:SS') trancdt, 'Transfer In ('||frm_user||')' tranc_type, (amount) amount, 0 bonus, 0 rollover, issueddt trxdt, '' category from wet_tp
       where status='P'

       union all

       select user_id, tranc_id, db_utfield(cs_approveby,'~',2) trancdt, 'Adjustment' tranc_type, 0 amount, bonus, 0 rollover, claimdt trxdt, adjustment_category category from wet_topup
       where claim='Y'
       and adjustment='Y'
       and nvl(adjustment_type,'U')='U'
       and status='P'

       Union all

       select user_id, tranc_id, to_char(claimdt,'DD-MON-YYYY HH24:MI:SS') trancdt, 'Advance' tranc_type, 0 amount, bonus, 0 rollover, claimdt trxdt, '' category from wet_topup
       where claim='Y'
       and adjustment='Y'
       and nvl(adjustment_type,'U')='A'
       and status='P'

       Union all

       select user_id, tranc_id, db_utfield(cs_approveby,'~',2) trancdt, 'Referer Bonus' tranc_type, 0 amount, bonus, 0 rollover, claimdt trxdt, '' category from wet_topup
       where adjustment='Y'
       and nvl(adjustment_type,'U')='R'
       and status='P'

       Union all

       select user_id, null tranc_id, to_char(trxdt,'DD-MON-YYYY HH24:MI:SS') trancdt, 'WL before Admin' tranc_type, wl amount, 0 bonus, 0 rollover, trxdt trxdt, '' category from wet_pwl
)
order by trxdt desc