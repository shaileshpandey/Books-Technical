-- ======================================== START Missing Indexes =======================================
SELECT
	'MISSING_INDEXES' 'MISSING INDEXES', 
	(MIGS.AVG_TOTAL_USER_COST * (MIGS.AVG_USER_IMPACT) * (MIGS.USER_SEEKS + MIGS.USER_SCANS)) /100 AS IMPROVEMENT_MEASURE,
  'CREATE INDEX [MISSING_INDEX_' + CONVERT (VARCHAR, MIG.INDEX_GROUP_HANDLE) + '_' + CONVERT (VARCHAR, MID.INDEX_HANDLE)
  + '_' + LEFT (PARSENAME(MID.STATEMENT, 1), 32) + ']'
  + ' ON ' + MID.STATEMENT
  + ' (' + ISNULL (MID.EQUALITY_COLUMNS,'')
    + CASE WHEN MID.EQUALITY_COLUMNS IS NOT NULL AND MID.INEQUALITY_COLUMNS IS NOT NULL THEN ',' ELSE '' END
    + ISNULL (MID.INEQUALITY_COLUMNS, '')
  + ')'
  + ISNULL (' INCLUDE (' + MID.INCLUDED_COLUMNS + ')', '') AS CREATE_INDEX_STATEMENT,
	MID.EQUALITY_COLUMNS,
	MID.INEQUALITY_COLUMNS,
	MID.INCLUDED_COLUMNS,
   MID.DATABASE_ID, MID.[OBJECT_ID],
   D.NAME,
   MIGS.*
FROM SYS.DM_DB_MISSING_INDEX_GROUPS MIG
INNER JOIN SYS.DM_DB_MISSING_INDEX_GROUP_STATS MIGS ON MIGS.GROUP_HANDLE = MIG.INDEX_GROUP_HANDLE
INNER JOIN SYS.DM_DB_MISSING_INDEX_DETAILS MID ON MIG.INDEX_HANDLE = MID.INDEX_HANDLE
INNER JOIN SYS.DATABASES D ON D.DATABASE_ID = MID.DATABASE_ID
WHERE D.NAME = DB_NAME() 
ORDER BY MIGS.AVG_TOTAL_USER_COST * MIGS.AVG_USER_IMPACT * (MIGS.USER_SEEKS + MIGS.USER_SCANS) DESC
-- ======================================== END Missing Indexes =======================================

GO

-- ======================================== START UNUSED Indexes =======================================
SELECT
'UNSED_INDEXES' 'UNSED_INDEXES',
o.name
, indexname=i.name
, i.index_id   
, reads=user_seeks + user_scans + user_lookups   
, writes =  user_updates   
, rows = (SELECT SUM(p.rows) FROM sys.partitions p WHERE p.index_id = s.index_id AND s.object_id = p.object_id)
, CASE
	WHEN s.user_updates < 1 THEN 100
	ELSE 1.00 * (s.user_seeks + s.user_scans + s.user_lookups) / s.user_updates
  END AS reads_per_write
, 'DROP INDEX ' + QUOTENAME(i.name) 
+ ' ON ' + QUOTENAME(c.name) + '.' + QUOTENAME(OBJECT_NAME(s.object_id)) as 'drop statement'
FROM sys.dm_db_index_usage_stats s  
INNER JOIN sys.indexes i ON i.index_id = s.index_id AND s.object_id = i.object_id   
INNER JOIN sys.objects o on s.object_id = o.object_id
INNER JOIN sys.schemas c on o.schema_id = c.schema_id
WHERE OBJECTPROPERTY(s.object_id,'IsUserTable') = 1
AND objectproperty(s.object_id, 'IsIndexable') = 1
AND objectproperty(s.object_id, 'IsIndexed') = 1
AND s.database_id = DB_ID()   
AND i.type_desc = 'nonclustered'
AND i.is_primary_key = 0
AND i.is_unique_constraint = 0
AND (SELECT SUM(p.rows) FROM sys.partitions p WHERE p.index_id = s.index_id AND s.object_id = p.object_id) > 10000
ORDER BY reads
-- ======================================== END UNUSED Indexes =======================================
GO

-- ======================================== START SLOW Queries =======================================
SELECT TOP 100
	  'SLOW_Query' 'SLOW_Query',
      QS.TOTAL_ELAPSED_TIME / QS.EXECUTION_COUNT / 1000000.0 AS AVERAGE_SECONDS,
      QS.TOTAL_ELAPSED_TIME / 1000000.0 AS TOTAL_SECONDS,
      QS.EXECUTION_COUNT,
      SUBSTRING (QT.TEXT,QS.STATEMENT_START_OFFSET/2,
      (CASE WHEN QS.STATEMENT_END_OFFSET = -1
      THEN LEN(CONVERT(NVARCHAR(MAX), QT.TEXT)) * 2
      ELSE QS.STATEMENT_END_OFFSET END - QS.STATEMENT_START_OFFSET)/2) AS INDIVIDUAL_QUERY,
      O.NAME AS OBJECT_NAME,
      DB_NAME(QT.DBID) AS DATABASE_NAME
FROM
      SYS.DM_EXEC_QUERY_STATS QS
      CROSS APPLY SYS.DM_EXEC_SQL_TEXT(QS.SQL_HANDLE) AS QT
      LEFT OUTER JOIN SYS.OBJECTS O ON QT.OBJECTID = O.OBJECT_ID
WHERE
      QT.DBID = DB_ID()
ORDER BY
      AVERAGE_SECONDS DESC;

-- ======================================== END SLOW Queries =======================================

-- ====================================== START OPEN Connection ====================================
-- Get a count of SQL connections by IP address
SELECT 'OPEN Connection' 'OPEN_CONNECTION', ec.client_net_address, es.[program_name], 
es.[host_name], es.login_name, 
COUNT(ec.session_id) AS [connection count] 
FROM sys.dm_exec_sessions AS es  
INNER JOIN sys.dm_exec_connections AS ec  
ON es.session_id = ec.session_id   
GROUP BY ec.client_net_address, es.[program_name], es.[host_name], es.login_name  
ORDER BY ec.client_net_address, es.[program_name];
-- ====================================== END OPEN Connection ====================================

-- ===================================== START MOST USED SP AND Functions ========================
SELECT DISTINCT  o.name ObjectName, deqs.execution_count
FROM sys.dm_exec_query_stats deqs
CROSS APPLY sys.dm_exec_sql_text (deqs.sql_handle) dest
JOIN sys.objects o ON o.object_id = dest.objectid
WHERE dest.dbid = DB_ID()
ORDER BY deqs.execution_count desc
-- ===================================== END MOST USED SP AND Functions ========================

-- ===================================== START Missing Dependencies ============================
select
	'Error' 'Error',
    object_name(referencing_id) as 'object making reference',
    referenced_class_desc,
    referenced_schema_name,
    referenced_entity_name as 'object name referenced',
    o.name as 'Object Not Found'
from sys.sql_expression_dependencies e
	left join sys.objects o on o.name = e.referenced_entity_name
    left join sys.tables t
    on e.referenced_entity_name = t.name
    WHERE o.object_id IS NULL
-- ===================================== END Missing Dependencies ============================