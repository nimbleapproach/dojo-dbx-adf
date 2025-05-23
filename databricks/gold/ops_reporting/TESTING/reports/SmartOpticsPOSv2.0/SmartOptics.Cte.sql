With ExchangeRates as (
SELECT 
	CONVERT(DATE,STARTDATE) AS StartDate
	,TOCURRENCY
	,FROMCURRENCY
	,RATE
FROM ExchangeRateEntityStaging 
	WHERE (FROMCURRENCY = 'USD')
),

ExchangeRates2 as 
(
SELECT 
	CONVERT(DATE,STARTDATE) AS StartDate
	,TOCURRENCY
	,FROMCURRENCY
	,RATE
FROM ExchangeRateEntityStaging 
	WHERE (FROMCURRENCY = 'GBP' AND TOCURRENCY = 'USD')
)