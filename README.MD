# SQL Server Vector Search Demo

This repository demonstrates vector search capabilities in SQL Server, featuring two key scripts:

1. **Vector Indexing and Search** - Fast similarity search using diskANN indexes
2. **Tiered Embedding Management** - Efficient storage of large-scale embeddings using external tables

## Features

- Vector data type operations with cosine, euclidean, and dot product metrics
- DiskANN vector indexing for high-performance similarity search
- S3-compatible external storage integration for tiered data management
- Transparent access to embeddings across multiple storage tiers
- Performance comparison between exact and approximate nearest neighbor algorithms

## Prerequisites

- SQL Server 2022 with vector capabilities
- S3-compatible object storage
- Trace flags 466, 474, and 13981

## Usage

1. Create a database named `StackOverflow_Embeddings`
2. Run vector-index.sql to set up vector functionality
3. Run managing-embeddings.sql to implement tiered storage

## Performance

Approximate nearest neighbors search (16ms) demonstrates a 55x performance improvement over exact search (886ms) when using vector indexes.

## Notes

The example uses StackOverflow data with 768-dimensional embeddings generated via the Ollama model on the Title column in the Posts table.