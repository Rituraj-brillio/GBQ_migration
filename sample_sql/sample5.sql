SELECT EventID, EventTime, Message, Node, IP_Address, Region
FROM "SNOWFLAKE_TO_GBQ"."NODE"."Node_with_IP"
WHERE Region = 'EMEA';
