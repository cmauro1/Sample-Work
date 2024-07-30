'''
Service Discrepancy Report

Here is a example of a report I built in SQL to help monitor the progress of data cleanup
for an acquisition that I migrated. 

The project relied on a complex series of Routesheets. Each of the 3 operating branches
had their own Excel workbook for every day of the week. Each workbook contained a worksheet
for every route being run out of that branch on that day. The Routesheets were loaded into
SQL Server using a Python script that I also wrote.

In order to get an accurate sense of how many active products were in the field, data
had to be aggregated. The data was maintained by hand which resulted in many entry errors. 
This report was instrumental in identifying all the obscure inconsistencies in the Routesheet data
so they could be sent back to the team for correction.
'''


WITH RouteServices AS 
(
			SELECT 
				   ROW_NUMBER() OVER (PARTITION BY Jobkey, ProductType, [Service], Latitude, Longitude						--Represents the number of occurences found in Routesheets for each unique Product
									  ORDER BY Jobkey, ProductType, [Service], Latitude, Longitude
						)																			AS Ord,
				   Branch, 
				   ProductType,

				   CASE																										--Quantity must be calculated because a product occurs in routesheets for each of its weekly services
						WHEN [Service] LIKE '_[xX]2%' THEN COUNT(ProductType) / (CAST(LEFT(TRIM([Service]),1) AS INT) * 2)  --When service is twice per day, but any number of days in a week
						WHEN [Service] LIKE '_[xX]%'  THEN COUNT(ProductType) / CAST(LEFT(TRIM([Service]),1) AS INT) 		--When service happens once in a day, but any number of days in a week 
													  ELSE COUNT(ProductType)   											--Less than weekly service and on-call should only occur once 

				   END																				AS Quantity,
				   JobKey,																									--Jobkey is a key used to represent an order
				   MAX(																										--Represents the expected number of occurences for each Product based on the Service Frequency
					   CASE 					
						WHEN [Service] LIKE '_[xX]2%'		THEN cast(LEFT(ServiceFrequency,1) as int) * 2
						WHEN [Service] LIKE '%EOW%' 		THEN 1
						WHEN [Service] LIKE '%As%needed%'	THEN 1
						WHEN [Service] LIKE '%On%call%'		THEN 1
						WHEN [Service] = ''					THEN 1
						WHEN [Service] = 'None'				THEN 1
						WHEN [Service] = '%OAM%'			THEN 1
						WHEN [Service] LIKE '1M'			THEN 1
						WHEN [Service] LIKE 'MONTHLY'		THEN 1
															ELSE CAST(LEFT(ServiceFrequency,1) AS INT) 
					  END
				     )																				AS MaxOrd,
				   ServiceDay
			FROM Routesheets 
			WHERE Jobkey LIKE '[0-9][^/]%' 																					--Found valid Jobkeys having a number in the first position and not having "/" in the second position
			  AND [Route] NOT LIKE '%Inactive%'																				--Exclude Routes marked Inactive
			GROUP BY Jobkey, ProductType, Branch, [Service], ServiceDay, Latitude, Longitude
		   )


SELECT Branch, 
	   JobKey, 
	   STRING_AGG(ServiceDay, ', ') AS ServiceDays, 
	   ProductType, Quantity, 
	   MAX(Ord) AS NumberOfServices, 
	   MaxOrd AS RequiredNumberOfServices
FROM RouteServices
GROUP BY Branch, Quantity, ProductType, JobKey, MaxOrd
HAVING MAX(Ord) != MaxOrd																									--Show products where current number of occurences do not match the expected number of occurrences
