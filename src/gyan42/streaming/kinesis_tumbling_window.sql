

-- ** Aggregate (COUNT, AVG, etc.) + Tumbling Time Window **
-- Performs function on the aggregate rows over a 10 second tumbling window for a specified column.
--          .----------.   .----------.   .----------.
--          |  SOURCE  |   |  INSERT  |   |  DESTIN. |
-- Source-->|  STREAM  |-->| & SELECT |-->|  STREAM  |-->Destination
--          |          |   |  (PUMP)  |   |          |
--          '----------'   '----------'   '----------'
-- STREAM (in-application): a continuously updated entity that you can SELECT from and INSERT into like a TABLE
-- PUMP: an entity used to continuously 'SELECT ... FROM' a source STREAM, and INSERT SQL results into an output STREAM
-- Create output stream, which can be used to send to a destination
CREATE OR REPLACE STREAM "DESTINATION_SQL_STREAM" (Location VARCHAR(64), "MOST_FREQUENT_VALUES" BIGINT);
-- Create a pump which continuously selects from a source stream (SOURCE_SQL_STREAM_001)
-- performs an aggregate count that is grouped by columns ticker over a 10-second tumbling window
-- and inserts into output stream (DESTINATION_SQL_STREAM)
CREATE OR REPLACE  PUMP "STREAM_PUMP" AS
    INSERT INTO "DESTINATION_SQL_STREAM"
    SELECT STREAM *
    FROM TABLE (TOP_K_ITEMS_TUMBLING(CURSOR (SELECT STREAM * from "SOURCE_SQL_STREAM_001"), 'COL_LocationDescription', 5, 60));

-- https://docs.aws.amazon.com/kinesisanalytics/latest/sqlref/top-k.html
-- TOP_K_ITEMS_TUMBLING (
--       in-application-streamPointer,
--       'columnName',
--       K,
--       windowSize,
--    )