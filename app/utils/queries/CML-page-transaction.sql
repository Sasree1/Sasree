select * from (
       select wt.user_id, wp.FLNAME, wt.tranc_id, to_char(wt.claimdt,'DD-MON-YYYY HH24:MI:SS') trancdt, 'Top Up' tranc_type, wt.amount from wet_topup wt
       join wem_playerdet wpd ON wt.user_id = wpd.user_id
       join wem_player wp ON wpd.id = wp.id
       where wt.claim='Y'
       and wt.status='P'
       and wt.adjustment='N'

       union all

       select ww.user_id, wp.FLNAME, ww.tranc_id, to_char(ww.issueddt,'DD-MON-YYYY HH24:MI:SS') trancdt, 'Withdraw' tranc_type, (ww.amount*-1) amount from wet_withdraw ww
       join wem_playerdet wpd ON ww.user_id = wpd.user_id
       join wem_player wp ON wpd.id = wp.id
       where ww.status='P'

       union all

       select wt.frm_user user_id, wp.FLNAME, wt.tranc_id, to_char(wt.issueddt,'DD-MON-YYYY HH24:MI:SS') trancdt, 'Transfer Out ('||wt.to_user||')' tranc_type, (wt.amount*-1) amount from wet_tp wt
       join wem_playerdet wpd ON wt.frm_user = wpd.user_id
       join wem_player wp ON wpd.id = wp.id
       where wt.status='P'

       union all
 
       select wt.to_user user_id, wp.FLNAME, wt.tranc_id, to_char(wt.issueddt,'DD-MON-YYYY HH24:MI:SS') trancdt, 'Transfer In ('||wt.frm_user||')' tranc_type, (wt.amount) amount from wet_tp wt
       join wem_playerdet wpd ON wt.to_user = wpd.user_id
       join wem_player wp ON wpd.id = wp.id
       where wt.status='P'

       union all

       select wt.user_id, wp.FLNAME, wt.tranc_id, db_utfield(wt.cs_approveby,'~',2) trancdt, 'Adjustment' tranc_type, 0 amount from wet_topup wt
       join wem_playerdet wpd ON wt.user_id = wpd.user_id
       join wem_player wp ON wpd.id = wp.id
       where wt.claim='Y'
       and wt.adjustment='Y'
       and nvl(wt.adjustment_type,'U')='U'
       and wt.status='P'

       Union all

       select wt.user_id, wp.FLNAME, wt.tranc_id, to_char(wt.claimdt,'DD-MON-YYYY HH24:MI:SS') trancdt, 'Advance' tranc_type, 0 amount from wet_topup wt
       join wem_playerdet wpd ON wt.user_id = wpd.user_id
       join wem_player wp ON wpd.id = wp.id
       where wt.claim='Y'
       and wt.adjustment='Y'
       and nvl(wt.adjustment_type,'U')='A'
       and wt.status='P'

       Union all

       select wt.user_id, wp.FLNAME, wt.tranc_id, db_utfield(wt.cs_approveby,'~',2) trancdt, 'Referer Bonus' tranc_type, 0 amount from wet_topup wt
       join wem_playerdet wpd ON wt.user_id = wpd.user_id
       join wem_player wp ON wpd.id = wp.id
       where wt.adjustment='Y'
       and nvl(wt.adjustment_type,'U')='R'
       and wt.status='P'
)
order by trancdt desc
-- where trancdt >= :TRANSACTION_FROM
