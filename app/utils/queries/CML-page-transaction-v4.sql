SELECT * FROM (
    SELECT wt.user_id, wp.FLNAME, wt.tranc_id,
           TO_CHAR(wt.claimdt, 'DD-MON-YYYY HH24:MI:SS') trancdt,
           'Top Up' tranc_type, wt.amount
    FROM wet_topup wt
    JOIN wem_playerdet wpd ON wt.user_id = wpd.user_id
    JOIN wem_player wp ON wpd.id = wp.id
    WHERE wt.claim = 'Y'
      AND wt.status = 'P'
      AND wt.adjustment = 'N'
      AND wt.claimdt BETWEEN TO_DATE('16-MAR-2025', 'DD-MON-YYYY') AND TO_DATE('16-APR-2025', 'DD-MON-YYYY')

    UNION ALL

    SELECT ww.user_id, wp.FLNAME, ww.tranc_id,
           TO_CHAR(ww.issueddt, 'DD-MON-YYYY HH24:MI:SS') trancdt,
           'Withdraw' tranc_type, (ww.amount * -1) amount
    FROM wet_withdraw ww
    JOIN wem_playerdet wpd ON ww.user_id = wpd.user_id
    JOIN wem_player wp ON wpd.id = wp.id
    WHERE ww.status = 'P'
      AND ww.issueddt BETWEEN TO_DATE('16-MAR-2025', 'DD-MON-YYYY') AND TO_DATE('16-APR-2025', 'DD-MON-YYYY')

    UNION ALL

    SELECT wt.frm_user user_id, wp.FLNAME, wt.tranc_id,
           TO_CHAR(wt.issueddt, 'DD-MON-YYYY HH24:MI:SS') trancdt,
           'Transfer Out (' || wt.to_user || ')' tranc_type, (wt.amount * -1) amount
    FROM wet_tp wt
    JOIN wem_playerdet wpd ON wt.frm_user = wpd.user_id
    JOIN wem_player wp ON wpd.id = wp.id
    WHERE wt.status = 'P'
      AND wt.issueddt BETWEEN TO_DATE('16-MAR-2025', 'DD-MON-YYYY') AND TO_DATE('16-APR-2025', 'DD-MON-YYYY')

    UNION ALL

    SELECT wt.to_user user_id, wp.FLNAME, wt.tranc_id,
           TO_CHAR(wt.issueddt, 'DD-MON-YYYY HH24:MI:SS') trancdt,
           'Transfer In (' || wt.frm_user || ')' tranc_type, (wt.amount) amount
    FROM wet_tp wt
    JOIN wem_playerdet wpd ON wt.to_user = wpd.user_id
    JOIN wem_player wp ON wpd.id = wp.id
    WHERE wt.status = 'P'
      AND wt.issueddt BETWEEN TO_DATE('16-MAR-2025', 'DD-MON-YYYY') AND TO_DATE('16-APR-2025', 'DD-MON-YYYY')

    UNION ALL

    SELECT wt.user_id, wp.FLNAME, wt.tranc_id,
           db_utfield(wt.cs_approveby, '~', 2) trancdt,
           'Adjustment' tranc_type, 0 amount
    FROM wet_topup wt
    JOIN wem_playerdet wpd ON wt.user_id = wpd.user_id
    JOIN wem_player wp ON wpd.id = wp.id
    WHERE wt.claim = 'Y'
      AND wt.adjustment = 'Y'
      AND NVL(wt.adjustment_type, 'U') = 'U'
      AND wt.status = 'P'
      AND TO_DATE(db_utfield(wt.cs_approveby, '~', 2), 'DD-MON-YYYY HH24:MI:SS')
          BETWEEN TO_DATE('16-MAR-2025', 'DD-MON-YYYY') AND TO_DATE('16-APR-2025', 'DD-MON-YYYY')

    UNION ALL

    SELECT wt.user_id, wp.FLNAME, wt.tranc_id,
           TO_CHAR(wt.claimdt, 'DD-MON-YYYY HH24:MI:SS') trancdt,
           'Advance' tranc_type, 0 amount
    FROM wet_topup wt
    JOIN wem_playerdet wpd ON wt.user_id = wpd.user_id
    JOIN wem_player wp ON wpd.id = wp.id
    WHERE wt.claim = 'Y'
      AND wt.adjustment = 'Y'
      AND NVL(wt.adjustment_type, 'U') = 'A'
      AND wt.status = 'P'
      AND wt.claimdt BETWEEN TO_DATE('16-MAR-2025', 'DD-MON-YYYY') AND TO_DATE('16-APR-2025', 'DD-MON-YYYY')

    UNION ALL

    SELECT wt.user_id, wp.FLNAME, wt.tranc_id,
           db_utfield(wt.cs_approveby, '~', 2) trancdt,
           'Referer Bonus' tranc_type, 0 amount
    FROM wet_topup wt
    JOIN wem_playerdet wpd ON wt.user_id = wpd.user_id
    JOIN wem_player wp ON wpd.id = wp.id
    WHERE wt.adjustment = 'Y'
      AND NVL(wt.adjustment_type, 'U') = 'R'
      AND wt.status = 'P'
      AND TO_DATE(db_utfield(wt.cs_approveby, '~', 2), 'DD-MON-YYYY HH24:MI:SS')
          BETWEEN TO_DATE('16-MAR-2025', 'DD-MON-YYYY') AND TO_DATE('16-APR-2025', 'DD-MON-YYYY')
)
ORDER BY trancdt DESC;
