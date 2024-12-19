with ExchangeRates as 
(
SELECT 
	CONVERT(DATE,STARTDATE) AS StartDate --removed correction on date?? 
	,TOCURRENCY
	,FROMCURRENCY
	,RATE
FROM ExchangeRateEntityStaging 
	WHERE (FROMCURRENCY = 'USD')
),
ExchangeRates2 as 
(
SELECT 
	CONVERT(DATE,STARTDATE) AS StartDate --removed correction on date?? 
	,TOCURRENCY
	,FROMCURRENCY
	,RATE
FROM ExchangeRateEntityStaging 
	WHERE (FROMCURRENCY = 'GBP' AND TOCURRENCY = 'USD')
)