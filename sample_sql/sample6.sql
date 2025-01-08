SELECT EventID, EventTime, Message, Node, IP_Address, City
FROM "SNOWFLAKE_TO_GBQ"."NODE"."Node_with_IP"
LIMIT 10;
