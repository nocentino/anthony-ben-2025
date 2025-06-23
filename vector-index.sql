------------------------------------------------------------
-- Step 1: Create a table with vector embeddings
------------------------------------------------------------
/*
 The current preview has the following limitations:
    * Vector index can't be partitioned. No partition support.
    * The table must have a single column, integer, primary key clustered index.
    * A table with a vector index becomes read only. No data modification is allowed while the vector index is present on the table.
    * Vector indexes aren't replicated to subscribers.
    * Please check the documentation for the latest updates.

    Docs:      https://learn.microsoft.com/en-us/sql/t-sql/statements/create-vector-index-transact-sql?view=sql-server-ver17
    Blog Post: https://devblogs.microsoft.com/azure-sql/efficiently-and-elegantly-modeling-embeddings-in-azure-sql-and-sql-server/
*/ 
USE [StackOverflow_Embeddings];
GO

CREATE TABLE dbo.PostEmbeddings (
    PostID    INT NOT NULL PRIMARY KEY CLUSTERED,   -- Foreign key to Posts table
    Embedding VECTOR(768) NOT NULL,                 -- Vector embeddings (768 dimensions)
    CreatedAt DATETIME NOT NULL DEFAULT GETDATE(),  -- Timestamp for when the embedding was created
    UpdatedAt DATETIME NULL                         -- Timestamp for when the embedding was last updated
) ON EmbeddingsFileGroup;                           -- Specify the filegroup
GO

------------------------------------------------------------
-- Step 2: Search and Vector Distance (Exact Nearest Neighbors)
------------------------------------------------------------
/*
 This demo demonstrates how to perform an exact similarity search using vector embeddings.
 Exact Nearest Neighbors (ENN) calculates the similarity score for all embeddings in the dataset,
 ensuring precise results but at the cost of higher computational overhead for large datasets.

 Key Steps:
 1. Generate an embedding for the query text using the AI_GENERATE_EMBEDDINGS function.
 2. Perform a similarity search by calculating the cosine distance between the query embedding
    and each embedding in the PostEmbeddings table.
 3. Order the results by cosine distance (lower distance = higher similarity).
*/
USE [StackOverflow_Embeddings];
GO

-- Turn on the IO and time Statistics
SET STATISTICS IO ON;
GO
SET STATISTICS TIME ON;
GO

-- Generate embedding for the query text using a pre-trained model
DECLARE @QueryText NVARCHAR(MAX) = N'Find me posts about issuses with SQL Server performance'; --<---this is intentionally misspelled to highlight the similarity search
DECLARE @QueryEmbedding VECTOR(768);

SET @QueryEmbedding = AI_GENERATE_EMBEDDINGS(@QueryText USE MODEL ollama);

-- Perform exact similarity search
SELECT TOP 10 
    p.Id, 
    p.Title, 
    pe.Embedding,
    vector_distance('cosine', @QueryEmbedding, pe.Embedding) AS SimilarityScore -- Calculate cosine distance, can also be 'euclidean' or 'dot''
FROM 
    dbo.Posts p
JOIN 
    dbo.PostEmbeddings pe ON p.Id = pe.PostID 
WHERE 
    pe.Embedding IS NOT NULL 
ORDER BY 
    SimilarityScore ASC; 

/*
    Table 'Posts'. Scan count 9, logical reads 483961, physical reads 0, page server reads 0, read-ahead reads 0, page server read-ahead reads 0, lob logical reads 0, lob physical reads 0, lob page server reads 0, lob read-ahead reads 0, lob page server read-ahead reads 0.
    Table 'PostEmbeddings'. Scan count 9, logical reads 367334, physical reads 0, page server reads 0, read-ahead reads 0, page server read-ahead reads 0, lob logical reads 0, lob physical reads 0, lob page server reads 0, lob read-ahead reads 0, lob page server read-ahead reads 0.
    Table 'Worktable'. Scan count 0, logical reads 0, physical reads 0, page server reads 0, read-ahead reads 0, page server read-ahead reads 0, lob logical reads 0, lob physical reads 0, lob page server reads 0, lob read-ahead reads 0, lob page server read-ahead reads 0.
    Table 'Worktable'. Scan count 0, logical reads 0, physical reads 0, page server reads 0, read-ahead reads 0, page server read-ahead reads 0, lob logical reads 0, lob physical reads 0, lob page server reads 0, lob read-ahead reads 0, lob page server read-ahead reads 0.

    SQL Server Execution Times:
    CPU time = 6516 ms, elapsed time = 886 ms.
    Total execution time: 00:00:01.254
*/

------------------------------------------------------------
-- Step 3: Create a vector index for faster similarity searches
------------------------------------------------------------
/*
 Trace flags 466, 474, and 13981 are required for vector index creation and operations.
 These trace flags enable the necessary features in SQL Server for handling vector data types
 and performing similarity searches efficiently.

 This takes about 17 minutes to create the vector index, on 700,000 rows.
*/
DBCC TRACEON (466, 474, 13981, -1);
GO

-- Verify trace flags are enabled
DBCC TRACESTATUS;
GO

/*
    Create a vector index for faster similarity searches on recent data.
*/
CREATE VECTOR INDEX vec_idx ON dbo.PostEmbeddings([Embedding])
WITH (
    metric = 'cosine', -- Similarity metric to use (can also be 'euclidean' or 'dot')
    type = 'diskann',  -- Type of vector index (diskann for disk-based ANN, currently the only option)
    maxdop = 16        -- Maximum degree of parallelism for index creation, or uses the instance MAXDOP setting
);
GO

/*
    Verify the vector index creation
    This query retrieves the vector index details for the PostEmbeddings table
*/
SELECT 
    vi.obj_id,
    vi.index_id,
    vi.index_type,
    vi.dist_metric
FROM 
    sys.indexes i 
INNER JOIN
    sys.vector_indexes as vi ON vi.obj_id = i.object_id AND vi.index_id = i.index_id
WHERE 
    obj_id = object_id('[dbo].[PostEmbeddings]')

------------------------------------------------------------
-- Step 4: Search and Vector Distance (Approximate Nearest Neighbors)
------------------------------------------------------------
/*
 This demo demonstrates how to perform an Approximate Nearest Neighbors (ANN) search
 using the VECTOR_SEARCH function. ANN is optimized for speed and scalability, making
 it suitable for large datasets where exact similarity searches may be computationally expensive.

 Key Steps:
 1. Generate an embedding for the query text using the AI_GENERATE_EMBEDDINGS function.
 2. Use VECTOR_SEARCH to find the top 10 most similar embeddings in the PostEmbeddings table.
 3. Join the results with the Posts table to retrieve post details like Title and ID.
 4. Order the results by cosine distance (lower distance = higher similarity).
*/

--  Generate embedding for the query text using a pre-trained model
DECLARE @QueryText NVARCHAR(MAX) = N'Find me posts about issuses with SQL Server performance'; --<---this is intentionally misspelled to highlight the similarity search
DECLARE @QueryEmbedding VECTOR(768);
SET @QueryEmbedding = AI_GENERATE_EMBEDDINGS(@QueryText USE MODEL ollama);

--  Perform approximate similarity search using the vector index
SELECT 
    p.Id, 
    p.Title, 
    pe.Embedding,
    s.distance AS SimilarityScore
FROM 
    VECTOR_SEARCH(
        TABLE = dbo.PostEmbeddings AS pe,   -- Table containing vector embeddings
        COLUMN = Embedding,                 -- Column storing the vector embeddings
        SIMILAR_TO = @QueryEmbedding,       -- Query embedding to compare against
        METRIC = 'cosine',                  -- Similarity metric (cosine distance)
        TOP_N = 10                          -- Number of nearest neighbors to retrieve
    ) AS s
JOIN 
    dbo.Posts p ON p.Id = pe.PostID         -- Join with Posts table to get post details
ORDER BY 
    s.distance;                             -- Lower distance indicates higher similarity

/*
    Table 'PostEmbeddings'. Scan count 0, logical reads 5960, physical reads 0, page server reads 0, read-ahead reads 0, page server read-ahead reads 0, lob logical reads 0, lob physical reads 0, lob page server reads 0, lob read-ahead reads 0, lob page server read-ahead reads 0.
    Table 'Posts'. Scan count 0, logical reads 40, physical reads 0, page server reads 0, read-ahead reads 0, page server read-ahead reads 0, lob logical reads 0, lob physical reads 0, lob page server reads 0, lob read-ahead reads 0, lob page server read-ahead reads 0.
    Table 'Worktable'. Scan count 21, logical reads 791, physical reads 0, page server reads 0, read-ahead reads 0, page server read-ahead reads 0, lob logical reads 0, lob physical reads 0, lob page server reads 0, lob read-ahead reads 0, lob page server read-ahead reads 0.
    Table 'vector_index_Graph_Edge_table_658101385_1152000'. Scan count 0, logical reads 93, physical reads 0, page server reads 0, read-ahead reads 0, page server read-ahead reads 0, lob logical reads 0, lob physical reads 0, lob page server reads 0, lob read-ahead reads 0, lob page server read-ahead reads 0.

    SQL Server Execution Times:
    CPU time = 31 ms, elapsed time = 16 ms.
    Total execution time: 00:00:00.238
*/

------------------------------------------------------------
-- Step 5: Summary of similarity metrics
------------------------------------------------------------
/*
 Summary of When to Use Each
 Metric          Focus                            Best For
 Cosine          Direction (ignores magnitude)    Text similarity, semantic search, embeddings with normalized vectors.
 Euclidean       Distance (magnitude + direction) Clustering, spatial data, numerical datasets.
 Dot Product     Magnitude + alignment            Ranking, recommendation systems, neural network outputs.

 Practical Examples
 - Use cosine to find semantically similar posts based on embeddings.
 - Use euclidean to cluster posts into groups based on their embeddings.
 - Use dot product to rank posts based on their relevance to a query embedding.
*/