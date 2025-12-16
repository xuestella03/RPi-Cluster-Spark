# Query Summaries

## About each query
- **Query 1**: Group by aggregation -- embarrassingly parallel but projection (select a subset of the attributes) requires more compute than 6
- **Query 3**: 3-way join with selective filter predicates
- **Query 5**: 6 tables with little filtering -- largest intermediate results
- **Query 6**: simple filter and scalar aggregation that is embarrassingly parallel

## What should we expect?
