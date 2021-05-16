DELETE FROM home T1
    USING   home T2
WHERE   T1.index < T2.index  -- delete the older versions
    AND T1.sagsnummer  = T2.sagsnummer; 