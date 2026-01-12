# Query Summaries

## About each query
- **Query 1**: Group by aggregation -- embarrassingly parallel but projection (select a subset of the attributes) requires more compute than 6. 
- **Query 3**: 3-way join with selective filter predicates
- **Query 5**: 6 tables with little filtering -- largest intermediate results
- **Query 6**: simple filter and scalar aggregation that is embarrassingly parallel

## What should we expect?

## Order of Execution
```
-- #6+7   SELECT DISTINCT department_id                                 
-- #1     FROM employees                                                
-- #2     JOIN orders ON customers.customer_id = orders.customer_id     
-- #3     WHERE salary > 3000                                          
-- #4     GROUP BY department 
-- #5     HAVING AVG(salary) > 5000 
-- #8     ORDER BY department 
-- #9     LIMIT 10 OFFSET 5 
-- #10    OFFSET 5 ROWS FETCH NEXT 10 ROWS ONLY; 
```