select * from (
       select wt.user_id, wp.FLNAME, wp.GENDER, wt.tranc_id, to_char(wt.claimdt,'DD-MON-YYYY HH24:MI:SS') trancdt, 'Top Up' tranc_type, wt.amount from wet_topup wt
       join wem_playerdet wpd ON wt.user_id = wpd.user_id
       join wem_player wp ON wpd.id = wp.id
       where claim='Y'
       and status='P'
       and adjustment='N'

       -- union all

       -- select ww.user_id, wp.FLNAME, wp.GENDER, ww.tranc_id, to_char(ww.issueddt,'DD-MON-YYYY HH24:MI:SS') trancdt, 'Withdraw' tranc_type, (ww.amount*-1) amount from wet_withdraw ww
       -- join wem_playerdet wpd ON ww.user_id = wpd.user_id
       -- join wem_player wp ON wpd.id = wp.id
       -- where status='P'

       -- union all

       -- select frm_user user_id, tranc_id, to_char(issueddt,'DD-MON-YYYY HH24:MI:SS') trancdt, 'Transfer Out ('||to_user||')' tranc_type, (amount*-1) amount from wet_tp
       -- where status='P'

       -- union all
 
       -- select to_user user_id, tranc_id, to_char(issueddt,'DD-MON-YYYY HH24:MI:SS') trancdt, 'Transfer In ('||frm_user||')' tranc_type, (amount) amount from wet_tp
       -- where status='P'

       -- union all

       -- select user_id, tranc_id, db_utfield(cs_approveby,'~',2) trancdt, 'Adjustment' tranc_type, 0 amount from wet_topup
       -- where claim='Y'
       -- and adjustment='Y'
       -- and nvl(adjustment_type,'U')='U'
       -- and status='P'

       -- Union all

       -- select user_id, tranc_id, to_char(claimdt,'DD-MON-YYYY HH24:MI:SS') trancdt, 'Advance' tranc_type, 0 amount from wet_topup
       -- where claim='Y'
       -- and adjustment='Y'
       -- and nvl(adjustment_type,'U')='A'
       -- and status='P'

       -- Union all

       -- select user_id, tranc_id, db_utfield(cs_approveby,'~',2) trancdt, 'Referer Bonus' tranc_type, 0 amount from wet_topup
       -- where adjustment='Y'
       -- and nvl(adjustment_type,'U')='R'
       -- and status='P'

       -- Union all

       -- select user_id, null tranc_id, to_char(trxdt,'DD-MON-YYYY HH24:MI:SS') trancdt, 'WL before Admin' tranc_type, wl amount from wet_pwl
)
where rownum <= 1000
order by trancdt desc
