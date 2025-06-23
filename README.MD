# SQL Server 2025 Demos

This repository demonstrates vector search capabilities in SQL Server, featuring two key scripts:

1. **Vector Indexing and Search** - Fast similarity search using diskANN indexes
2. **Tiered Embedding Management** - Efficient storage of large-scale embeddings using external tables

## Features

- Vector data type operations with cosine, euclidean, and dot product metrics
- DiskANN vector indexing for high-performance similarity search
- S3-compatible external storage integration for tiered data management
- Transparent access to embeddings across multiple storage tiers
- Performance comparison between exact and approximate nearest neighbor algorithms

## Notes

The example uses StackOverflow data with 768-dimensional embeddings generated via the Ollama model on the Title column in the Posts table.
