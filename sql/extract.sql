```sql
SELECT unidade_acervo, COUNT(*) AS total
FROM consultas_fbn_tratada
GROUP BY unidade_acervo
ORDER BY total DESC;
```
