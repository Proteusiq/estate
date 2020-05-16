SELECT buildyear
	 ,price
	 ,downpayment
	 ,rooms
	 ,"size"
	 ,lotsize
	 ,street
	 ,daysforsale
	 ,isactive 
FROM recent_boligs
WHERE daysforsale > 0
  AND zipcode=2650
  AND rooms > 3
  AND size > 112
  AND price < 4.6 * 1000000
ORDER BY price DESC