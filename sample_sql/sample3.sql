SELECT EventID, EventTime, Message, Node, IP_Address
FROM "SNOWFLAKE_TO_GBQ"."NODE"."Node_with_IP"
WHERE EventTime BETWEEN '2024-05-01' AND '2024-05-07';
