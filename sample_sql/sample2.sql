SELECT EventID, EventTime, Node, IP_Address, Country, Status
FROM "SNOWFLAKE_TO_GBQ"."NODE"."Node_with_IP"
WHERE Country = 'Turkey' AND IP_Address = '10.0.8.87';
