SELECT EventID, EventTime, Message, Node, IP_Address
FROM "SNOWFLAKE_TO_GBQ"."NODE"."Node_with_IP"
WHERE Message LIKE '%100%';
