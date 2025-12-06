-- 4.1.1 Pré-visualização dos dados brutos
SELECT * 
FROM consultas_fbn_bruta
LIMIT 20;

-- 4.1.2 Estrutura da tabela bruta
DESCRIBE TABLE consultas_fbn_bruta;

-- 4.1.3 Contagem de registros
SELECT COUNT(*) AS total_registros
FROM consultas_fbn_bruta;
