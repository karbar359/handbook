CREATE EXTERNAL TABLE IF NOT EXISTS database_name.table_name (
    column_name_string STRING,
    column_name_bigint BIGINT,
    column_name_decimal DECIMAL(20, 2)
    )
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
LOCATION 'hdfs_path'

ALTER TABLE database_name.table_name DROP IF EXISTS PARTITION(dt="2020-01-01")

SELECT
FROM
JOIN
WHERE
GROUP BY
ORDER BY
LIMIT
