-- Demo - Using external tables to store embeddings
-- This demo showcases SQL Server's external table capabilities
-- to efficiently tier and manage large-scale AI vector embeddings
------------------------------------------------------------
-- Step 1: Set up authentication for external storage
------------------------------------------------------------
USE StackOverflow_Embeddings;
GO

/*
    Create a master key with a secure password.
*/
CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'YourStrongPassword123!';
GO

/*
    Create a database scoped credential for accessing external object storage.
*/
CREATE DATABASE SCOPED CREDENTIAL ExternalStorageCredential
WITH 
    IDENTITY = 'S3 Access Key', -- Use 'S3 Access Key' for S3-compatible storage
    SECRET = 'PSFBSAZRHDBIJOIPAPKLOACBOAJCMKCDIJFPGBNNLI:A5AF16F59832ac290/a0ab+5F915B1F79b8db93IKAE';
GO

------------------------------------------------------------
-- Step 2: Configure external data source and file format
------------------------------------------------------------
/*
    Create an external data source pointing to the storage location.
*/
CREATE EXTERNAL DATA SOURCE ExternalStorageSource
WITH (
    LOCATION = 's3://s200.fsa.lab/aen-sql-datavirt', -- S3 endpoint
    CREDENTIAL = ExternalStorageCredential
);
GO

/*
    Create a file format for Parquet files.
*/
CREATE EXTERNAL FILE FORMAT ParquetFileFormat
WITH (
    FORMAT_TYPE = PARQUET
);
GO

------------------------------------------------------------
-- Step 3: Enable and configure PolyBase for external data access
------------------------------------------------------------
/*
    Enable advanced options for PolyBase configuration.
*/
EXEC sp_configure 'show advanced options', 1;
RECONFIGURE;
GO

/*
    Enable PolyBase export for external table creation.
*/
EXEC sp_configure 'allow polybase export', 1;
RECONFIGURE;
GO

------------------------------------------------------------
-- Step 4: Create sample external table for testing
------------------------------------------------------------
/*
    Create an initial external table to validate the configuration.
*/
CREATE EXTERNAL TABLE PostEmbeddingsExternal
WITH (
    LOCATION = '/', -- Path within the external storage
    DATA_SOURCE = ExternalStorageSource,
    FILE_FORMAT = ParquetFileFormat
)
AS
SELECT TOP 10
    PostID,
    Embedding, -- Embedding vector from the PostEmbeddings table
    CreatedAt,
    UpdatedAt
FROM dbo.PostEmbeddings;
GO

-- Verify the external table can be queried
SELECT * FROM PostEmbeddingsExternal;

------------------------------------------------------------
-- Step 5: Analyze data distribution by year
------------------------------------------------------------
/*
    Analyze post distribution by year to plan our data tiering strategy.
*/
USE StackOverflow_Embeddings;
GO

SELECT 
    YEAR(CreationDate) AS PostYear, -- Extract the year from the CreatedDate column
    COUNT(*) AS PostCount -- Count the number of posts for each year
FROM 
    dbo.Posts INNER JOIN PostEmbeddings pe ON Posts.Id = pe.PostID
GROUP BY 
    YEAR(CreationDate)
ORDER BY 
    PostYear;

------------------------------------------------------------
-- Step 6: Implement year-based tiering to external storage
------------------------------------------------------------
/*
    Create external tables for each year of posts.
*/
DECLARE @StartYear INT = 2008;
DECLARE @EndYear INT = YEAR(GETDATE());
DECLARE @Year INT = @StartYear;
DECLARE @SQL NVARCHAR(MAX);

WHILE @Year <= @EndYear
BEGIN
    -- Generate the CETAS statement for the current year
    SET @SQL = N'
    CREATE EXTERNAL TABLE PostEmbeddings_' + CAST(@Year AS NVARCHAR(4)) + N'
    WITH (
        LOCATION = ''/PostEmbeddings_Archive/' + CAST(@Year AS NVARCHAR(4)) + N'/'',
        DATA_SOURCE = ExternalStorageSource,
        FILE_FORMAT = ParquetFileFormat
    )
    AS
    SELECT 
        PostID,
        Embedding,
        CreatedAt,
        UpdatedAt
    FROM dbo.PostEmbeddings INNER JOIN dbo.Posts ON PostEmbeddings.PostID = Posts.Id
    WHERE YEAR(CreationDate) = ' + CAST(@Year AS NVARCHAR(4)) + N';
    ';

    -- Execute the CETAS statement
    EXEC sp_executesql @SQL;

    -- Move to the next year
    SET @Year = @Year + 1;
    PRINT 'Created external table for year ' + CAST(@Year - 1 AS NVARCHAR(4));
END;
GO

------------------------------------------------------------
-- Step 7: Create optimized storage for recent data
------------------------------------------------------------
/*
    Create a table to hold recent data (2022 and later) for optimal performance.
*/
CREATE TABLE dbo.PostEmbeddings_2022_AndLater (
    PostID    INT NOT NULL PRIMARY KEY CLUSTERED,   -- Foreign key to Posts table
    Embedding VECTOR(768) NOT NULL,                 -- Vector embeddings (768 dimensions)
    CreatedAt DATETIME NOT NULL DEFAULT GETDATE(),  -- Timestamp for when the embedding was created
    UpdatedAt DATETIME NULL                         -- Timestamp for when the embedding was last updated
) ON EmbeddingsFileGroup;                           -- Specify the filegroup
GO

/*
    Copy recent records from the original table.
*/
INSERT INTO dbo.PostEmbeddings_2022_AndLater (PostID, Embedding, CreatedAt, UpdatedAt)
SELECT PostID, Embedding, CreatedAt, UpdatedAt
FROM dbo.PostEmbeddings pe INNER JOIN dbo.Posts p ON pe.PostID = p.Id
WHERE p.CreationDate >= '2022-01-01';


-- After migration, drop the original table that contained all data
DROP TABLE dbo.PostEmbeddings;
GO


--Rename the new table to the original name for compatibility
EXEC sp_rename 'dbo.PostEmbeddings_2022_AndLater', 'PostEmbeddings';
GO
------------------------------------------------------------
-- Step 8: Create a unified view across all data sources
------------------------------------------------------------
/*
    Create a view to provide transparent access across all data sources.
*/
CREATE VIEW dbo.PostEmbeddings_All
AS
SELECT PostID, Embedding, CreatedAt, UpdatedAt FROM dbo.PostEmbeddings
UNION ALL
SELECT PostID, Embedding, CreatedAt, UpdatedAt FROM dbo.PostEmbeddings_2021 
UNION ALL
SELECT PostID, Embedding, CreatedAt, UpdatedAt FROM dbo.PostEmbeddings_2020
UNION ALL
SELECT PostID, Embedding, CreatedAt, UpdatedAt FROM dbo.PostEmbeddings_2019
UNION ALL
SELECT PostID, Embedding, CreatedAt, UpdatedAt FROM dbo.PostEmbeddings_2018
UNION ALL
SELECT PostID, Embedding, CreatedAt, UpdatedAt FROM dbo.PostEmbeddings_2017
UNION ALL
SELECT PostID, Embedding, CreatedAt, UpdatedAt FROM dbo.PostEmbeddings_2016
UNION ALL
SELECT PostID, Embedding, CreatedAt, UpdatedAt FROM dbo.PostEmbeddings_2015
UNION ALL
SELECT PostID, Embedding, CreatedAt, UpdatedAt FROM dbo.PostEmbeddings_2014
UNION ALL
SELECT PostID, Embedding, CreatedAt, UpdatedAt FROM dbo.PostEmbeddings_2013
UNION ALL
SELECT PostID, Embedding, CreatedAt, UpdatedAt FROM dbo.PostEmbeddings_2012
UNION ALL
SELECT PostID, Embedding, CreatedAt, UpdatedAt FROM dbo.PostEmbeddings_2011
UNION ALL
SELECT PostID, Embedding, CreatedAt, UpdatedAt FROM dbo.PostEmbeddings_2010
UNION ALL
SELECT PostID, Embedding, CreatedAt, UpdatedAt FROM dbo.PostEmbeddings_2009
UNION ALL
SELECT PostID, Embedding, CreatedAt, UpdatedAt FROM dbo.PostEmbeddings_2008
GO

------------------------------------------------------------
-- Step 9: Verify data accessibility post-migration
------------------------------------------------------------
/*
    Verify all data remains accessible through the view.
    Notice the MAXDOP option to optimize parallel processing.
*/
USE StackOverflow_Embeddings
GO
SELECT 
    YEAR(CreationDate) AS PostYear,
    COUNT(*) AS PostCount
FROM 
    dbo.Posts INNER JOIN PostEmbeddings_All pe ON Posts.Id = pe.PostID
GROUP BY 
    YEAR(CreationDate)
ORDER BY 
    PostYear
OPTION (MAXDOP 16);

------------------------------------------------------------
-- Step 10: Analyze storage efficiency
------------------------------------------------------------
-- Check the size of the new table, which should be significantly smaller than the original PostEmbeddings table
EXEC sp_spaceused N'dbo.PostEmbeddings';
GO

-- Shrink the file to reclaim space after the migration
DBCC SHRINKFILE (N'StackOverflowEmbeddings' , 2695)
GO

-------------------------------------------------------------
-- Step 11: Perform a similarity search across all data on all tiers
--------------------------------------------------------------
DECLARE @QueryText NVARCHAR(MAX) = N'Find me posts about issuses with SQL Server performance'; --<---this is intentionally misspelled to highlight the similarity search
DECLARE @QueryEmbedding VECTOR(768);

SET @QueryEmbedding = AI_GENERATE_EMBEDDINGS(@QueryText USE MODEL ollama);

-- Perform similarity search
SELECT TOP 10 
    p.Id, 
    p.Title, 
    pe.Embedding,
    vector_distance('cosine', @QueryEmbedding, pe.Embedding) AS SimilarityScore
FROM 
    dbo.Posts p
JOIN 
    dbo.PostEmbeddings_All pe ON p.Id = pe.PostID 
WHERE 
    pe.Embedding IS NOT NULL 
ORDER BY 
    SimilarityScore ASC;
GO

-------------------------------------------------------------
-- Next Steps:
-- 1. Add a diskann index to the PostEmbeddings table for faster similarity searches.
-- 2. Configure other instance of SQL Server to use the same external storage for PostEmbeddings.
-- 3. Implement a data retention policy to archive or delete old embeddings.
--------------------------------------------------------------

