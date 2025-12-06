-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Título do Projeto: Teste CSV  Surdos
-- MAGIC ## Subtítulo Técnico: MVP — Engenharia de Dados (Sprint 1)
-- MAGIC
-- MAGIC ### Índice do Notebook
-- MAGIC
-- MAGIC 1. **Planejamento e Objetivo**
-- MAGIC 2. **Coleta**
-- MAGIC 3. **Modelagem Conceitual**
-- MAGIC 4. **ETL (Extract, Transform, Load)**
-- MAGIC 5. **Análise dos Dados e Métricas**
-- MAGIC 6. **Governança, Linhagem e Ética**
-- MAGIC 7. **Autoavaliação**
-- MAGIC 8. **Reprodutibilidade e Informações Finais**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1) Planejamento e Objetivo
-- MAGIC
-- MAGIC ### 1.1 Objetivo
-- MAGIC Este exercício tem como objetivo **praticar o fluxo de criação de um pipeline de dados** na plataforma Databricks, compreendendo as etapas de:
-- MAGIC - definição do problema,
-- MAGIC - identificação da fonte de dados,
-- MAGIC - coleta inicial,
-- MAGIC - preparação do ambiente para ETL simples.
-- MAGIC
-- MAGIC Nesta etapa, ainda não estamos realizando análise — apenas garantindo que o fluxo inicial de planejamento esteja documentado e funcionando no notebook.
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC ### 1.2 Problema de pesquisa
-- MAGIC Problema exploratório: *Como funciona e como registrar evidências da etapa de ingestão e documentação de fontes para um MVP?*
-- MAGIC
-- MAGIC Este treino serve para desenvolver conforto na plataforma, antes de aplicar o processo ao dataset oficial do trabalho final.
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC ### 1.3 Perguntas de análise
-- MAGIC Como inserir markdown no notebook?
-- MAGIC Como estruturar os tópicos?
-- MAGIC Como subir o csv?
-- MAGIC Como rodar o sql nos blocos do notebook?
-- MAGIC Como usar a ferramenta de visualização de informação do databricks?
-- MAGIC Como exportar meu notebook em html?
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC ### 1.4 Dataset selecionado
-- MAGIC **Nome do arquivo:** atendimento-a-comunidade-surda-em-sala-de-aula.csv  
-- MAGIC Fonte: Portal Brasileiro de Dados Abertos (dados.gov.br)  
-- MAGIC Descrição: Dataset que apresenta informações sobre atendimentos realizados por intérpretes de Libras em cursos de graduação e pós-graduação.
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC ### 1.5 Pipeline do projeto (fluxo conceitual)
-- MAGIC - Documentar a seleção do dataset de treino  
-- MAGIC - Registrar as informações básicas sobre a fonte  
-- MAGIC - Descrever limites, premissas e critérios  
-- MAGIC - Preparar o ambiente para carregar o CSV na etapa seguinte  
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC ### 1.6 Escopo e Limitações
-- MAGIC
-- MAGIC **Escopo deste MVP**
-- MAGIC - Construir um pipeline simples de ingestão → transformação → análise.
-- MAGIC - Utilizar exclusivamente SQL no Databricks.
-- MAGIC - Documentar todas as etapas utilizando Markdown.
-- MAGIC - Produzir ao menos uma tabela final tratada para análise.
-- MAGIC - Gerar visualizações simples (no Databricks ou Power BI).
-- MAGIC
-- MAGIC **Não fazem parte deste MVP (por decisão de escopo):**
-- MAGIC - Construção de modelo estrela completo (DW).
-- MAGIC - Uso de Python avançado ou bibliotecas externas.
-- MAGIC - Automação de pipelines via Jobs ou Workflows.
-- MAGIC - Integração contínua ou orquestração.
-- MAGIC
-- MAGIC **Limitações observadas**
-- MAGIC - O dataset selecionado inicialmente é pequeno, portanto a análise servirá como teste de fluxo.
-- MAGIC - A ausência de variáveis mais ricas reduz a complexidade das perguntas exploratórias.
-- MAGIC - Dados podem conter categorias heterogêneas ou campos vagos, exigindo padronização.
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC ### 1.7 Premissas
-- MAGIC
-- MAGIC - Todos os dados utilizados são **públicos** e não contêm informações pessoais sensíveis.
-- MAGIC - O objetivo do MVP é **demonstrar um pipeline funcional**, não criar um Data Warehouse completo.
-- MAGIC - A tabela bruta será **mantida sem alterações**, respeitando o princípio de preservação da fonte.
-- MAGIC - Transformações aplicadas serão documentadas claramente no notebook.
-- MAGIC - O modelo de dados final será uma **flat table**, suficiente para análises exploratórias.
-- MAGIC - Limitações da Databricks Free  Edition (como storage reduzido e clusters compartilhados) serão consideradas.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2) Coleta
-- MAGIC
-- MAGIC ### 2.1 Listagem e Validação Inicial das Fontes (1ª Triagem) "levantamento bibliográfico" EDITAR
-- MAGIC
-- MAGIC Antes da seleção do dataset final, foi realizada uma varredura no Portal Brasileiro de Dados Abertos (dados.gov.br) com o objetivo de identificar fontes compatíveis com a proposta da MVP. Para este MVP, a busca inicial pelos dados foi feita em portais oficiais de Dados Abertos, priorizando fontes governamentais confiáveis e com documentação mínima. Foram consultados:
-- MAGIC
-- MAGIC As seguintes bases foram mapeadas e avaliadas:
-- MAGIC
-- MAGIC - Sistema Nacional de Informações Culturais – SNIIC  
-- MAGIC - IBRAM – Museus  
-- MAGIC - Sistema de Informações e Indicadores Culturais – IBGE  
-- MAGIC - Conjuntos relacionados à acessibilidade digital (volumes muito reduzidos)  
-- MAGIC - Fundação Casa de Rui Barbosa – Acervo e Visitação (dados agregados, baixa granularidade)  
-- MAGIC - Biblioteca Nacional – Obras Consultadas por Pesquisadores (2016–2024)
-- MAGIC - Portal Brasileiro de Dados Abertos (dados.gov.br)
-- MAGIC - Ministério da Educação (MEC)
-- MAGIC - Ministério da Cultura (MinC)
-- MAGIC - SNIIC – Sistema Nacional de Informações Culturais
-- MAGIC
-- MAGIC **Resultado da 1ª triagem:**  
-- MAGIC Diversas bases apresentaram limitações, como poucas linhas, ausência de variáveis suficientes para análise exploratória, ou temática inadequada para responder às perguntas definidas na etapa de planejamento.  
-- MAGIC A partir dessa primeira triagem, foi identificado um dataset compatível com o objetivo experimental de treino, contendo informações sobre atendimentos à comunidade surda em sala de aula.
-- MAGIC Assim, foram mantidas apenas as fontes com potencial real de análise para a 2ª triagem.
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC ### 2.2 Critérios de Seleção das Fontes (2ª Triagem) EDITAR
-- MAGIC Antes de iniciar a coleta, estabeleci critérios para escolha do dataset a ser utilizado no MVP. Os critérios foram definidos para garantir relevância temática, qualidade mínima dos dados e compatibilidade com o pipeline de análise proposto.
-- MAGIC Após a validação inicial, os datasets remanescentes foram avaliados com critérios objetivos para definir quais realmente poderiam ser utilizados no MVP. Os critérios adotados na segunda triagem foram:
-- MAGIC
-- MAGIC - Disponibilidade em **formato CSV**  
-- MAGIC - **Volume mínimo** de registros (preferencialmente acima de 200 linhas)  
-- MAGIC - Existência de **variáveis relevantes** (textuais, numéricas, categóricas e/ou temporais)  
-- MAGIC - **Documentação mínima** ou descrição oficial da base  
-- MAGIC - Possibilidade de análise usando **SQL** exclusivamente  
-- MAGIC - **Licença aberta**, permitindo uso acadêmico  
-- MAGIC - **Ausência de dados pessoais sensíveis**  
-- MAGIC - Estrutura adequada para construção de pipeline simples (ingestão → transformação → análise)
-- MAGIC
-- MAGIC **Resultado da 2ª triagem:**  
-- MAGIC Após aplicar esses critérios, duas bases permaneceram como finalistas:  
-- MAGIC 1) Conjuntos da Fundação Casa de Rui Barbosa (visitação e acervo)  
-- MAGIC 2) Obras Consultadas por Pesquisadores — Fundação Biblioteca Nacional (2016–2024)
-- MAGIC
-- MAGIC A partir delas, foi realizada a escolha final do dataset.
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC ### 2.3 Critérios da Seleção Final do Dataset EDITAR
-- MAGIC Após a segunda triagem, foram aplicados critérios específicos para a escolha do dataset definitivo utilizado nesta MVP. A seleção final considerou:
-- MAGIC A escolha da base de dados seguiu critérios objetivos documentados previamente:
-- MAGIC
-- MAGIC - Maior **volume de registros**, permitindo análises estatísticas mínimas.  
-- MAGIC - Presença de **variáveis textuais, categóricas, numéricas e temporais**, ampliando as possibilidades de exploração.  
-- MAGIC - Estrutura mais próxima de uma **tabela relacional**, facilitando o uso das técnicas de SQL estudadas.  
-- MAGIC - **Granularidade adequada** (registro por obra consultada, não dados agregados).  
-- MAGIC - Dados disponíveis em **múltiplos anos**, possibilitando análises comparativas.  
-- MAGIC - Relevância informacional e documental para o contexto cultural brasileiro.  
-- MAGIC - Base amplamente utilizada e **bem documentada** no portal de dados abertos.  
-- MAGIC
-- MAGIC
-- MAGIC **1. Alinhamento temático**  
-- MAGIC Dados relacionados a acessibilidade, inclusão comunicacional ou atendimento especializado.
-- MAGIC
-- MAGIC **2. Disponibilidade em formato aberto**  
-- MAGIC Prioridade para CSV, por ser compatível com ingestão direta no Databricks.
-- MAGIC
-- MAGIC **3. Documentação mínima**  
-- MAGIC Descrição, origem e metadados básicos disponíveis na fonte.
-- MAGIC
-- MAGIC **4. Granularidade suficiente**  
-- MAGIC Presença de colunas relevantes para perguntas simples de análise.
-- MAGIC
-- MAGIC **5. Licença de uso**  
-- MAGIC Preferência por dados públicos/abertos sem restrição de reutilização.
-- MAGIC
-- MAGIC **6. Tamanho viável para processamento na Free Edition**  
-- MAGIC Bases pequenas ou médias (evitando datasets de milhões de linhas nesta primeira prática).
-- MAGIC
-- MAGIC 1. **Aderência ao objetivo**  
-- MAGIC    O dataset deve permitir responder às perguntas definidas no objetivo do MVP.  
-- MAGIC    (Ex.: temas de acessibilidade, inclusão, serviços culturais, educação ou atendimento ao público.)
-- MAGIC
-- MAGIC 2. **Disponibilidade em formato aberto**  
-- MAGIC    Prioridade para datasets oferecidos em **CSV**, evitando formatos dependentes de software proprietário.
-- MAGIC
-- MAGIC 3. **Documentação mínima**  
-- MAGIC    Datasets com descrição clara, dicionário de dados, fonte institucional identificada e metadados básicos.
-- MAGIC
-- MAGIC 4. **Atualização e período coberto**  
-- MAGIC    Preferência por bases recentes ou com escopo temporal definido.
-- MAGIC
-- MAGIC 5. **Granularidade suficiente**  
-- MAGIC    Deve possuir colunas relevantes para análise descritiva e geração de métricas simples.
-- MAGIC
-- MAGIC 6. **Licença de uso**  
-- MAGIC    Dados públicos com autorização clara de utilização (dados abertos governamentais).
-- MAGIC
-- MAGIC 7. **Tamanho viável**  
-- MAGIC    Arquivo com volume adequado para manipulação no Databricks Community Edition, evitando datasets gigantes.
-- MAGIC
-- MAGIC **Dataset selecionado:**  
-- MAGIC **Obras Consultadas por Pesquisadores — Fundação Biblioteca Nacional (2016–2024)**
-- MAGIC
-- MAGIC A base apresenta diversidade de campos, bom volume e potencial claro para construção de um pipeline completo (ingestão, transformação e análise), justificando sua escolha para esta MVP.
-- MAGIC Esses critérios foram utilizados para selecionar o dataset final, assegurando compatibilidade com o pipeline ETL e com o ambiente em nuvem adotado.
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC ### 2.4 Descrição do Dataset Selecionado
-- MAGIC
-- MAGIC **Dataset:** Obras Consultadas por Pesquisadores — Fundação Biblioteca Nacional (2016–2024)  
-- MAGIC **Origem:** Fundação Biblioteca Nacional  
-- MAGIC **Plataforma:** Portal Brasileiro de Dados Abertos (dados.gov.br)  
-- MAGIC **Formato:** CSV  
-- MAGIC **Licença:** Dados Abertos do Governo Federal  
-- MAGIC **Tema:** Registros de obras consultadas presencialmente na sede da FBN.
-- MAGIC
-- MAGIC **Descrição geral do conteúdo:**  
-- MAGIC O dataset reúne informações sobre consultas presenciais realizadas por pesquisadores, contendo dados bibliográficos, características físicas e administrativas das obras, além de registros de datas e horários de atendimento.
-- MAGIC
-- MAGIC As variáveis observadas incluem:
-- MAGIC
-- MAGIC - Título da obra  
-- MAGIC - Tipo  
-- MAGIC - Material  
-- MAGIC - Tombo  
-- MAGIC - Ano de publicação  
-- MAGIC - Volume  
-- MAGIC - Edição/Número  
-- MAGIC - Unidade de acervo  
-- MAGIC - Estado físico  
-- MAGIC - Data de início da consulta  
-- MAGIC - Data de devolução  
-- MAGIC - Tipo de circulação  
-- MAGIC - Hora de início da consulta  
-- MAGIC - Hora de devolução  
-- MAGIC
-- MAGIC Esses campos permitem análises temporais, comparativas e categóricas sobre o comportamento de uso do acervo.
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC ### 2.5 Justificativa da Escolha
-- MAGIC
-- MAGIC O dataset “Obras Consultadas por Pesquisadores — Fundação Biblioteca Nacional (2016–2024)” foi selecionado por atender plenamente aos requisitos definidos na etapa de planejamento e às necessidades de um pipeline mínimo viável.
-- MAGIC
-- MAGIC Os principais fatores para a escolha foram:
-- MAGIC
-- MAGIC - **Volume adequado de registros**, permitindo análises significativas.  
-- MAGIC - **Diversidade de variáveis** (textuais, categóricas, numéricas e temporais), possibilitando múltiplos tipos de consultas em SQL.
-- MAGIC - **Estrutura granular**, com cada linha representando uma obra consultada, favorecendo agregações, contagens e análises de frequência.
-- MAGIC - **Presença de campos de data e horário**, úteis para análises temporais básicas.  
-- MAGIC - **Formato CSV**, facilitando ingestão direta no Databricks.  
-- MAGIC - **Tema documental relevante**, compatível com práticas arquivísticas e de gestão de acervo.  
-- MAGIC - **Qualidade estrutural**, com dados normalmente bem formatados, reduzindo risco de falhas na implantação do pipeline.
-- MAGIC
-- MAGIC Essas características tornam a base adequada para demonstração de coleta, transformação e análise utilizando SQL em ambiente de nuvem.
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC ### 2.6 Estrutura das Colunas Principais
-- MAGIC
-- MAGIC A partir da inspeção inicial do arquivo CSV, é possível identificar as principais variáveis disponibilizadas no dataset. Esses campos descrevem características bibliográficas, administrativas e temporais das obras consultadas.
-- MAGIC
-- MAGIC **Colunas principais presentes no dataset:**
-- MAGIC
-- MAGIC - **Título** — Nome da obra consultada.  
-- MAGIC - **Tipo** — Categoria geral da obra (ex.: periódico, manuscrito).  
-- MAGIC - **Material** — Tipo de suporte ou natureza material do item.  
-- MAGIC - **Tombo** — Identificador único no acervo.  
-- MAGIC - **Ano** — Ano da publicação da obra.  
-- MAGIC - **Volume** — Informação sobre o volume, quando aplicável.  
-- MAGIC - **Edicao/Numero** — Número ou edição da obra.  
-- MAGIC - **Unidade de acervo** — Unidade física responsável pela guarda do item.  
-- MAGIC - **Estado físico** — Condição de conservação do material.  
-- MAGIC - **Data de início da consulta** — Data em que o item foi disponibilizado ao pesquisador.  
-- MAGIC - **Data de devolução** — Data em que o item foi devolvido no balcão.  
-- MAGIC - **Tipo de circulação** — Natureza do empréstimo/consulta (presencial, balcão etc.).  
-- MAGIC - **Hora de início da consulta** — Horário de início do atendimento.  
-- MAGIC - **Hora de devolução** — Horário de finalização da consulta.
-- MAGIC
-- MAGIC Esses campos oferecem material suficientemente variado para análises exploratórias, possibilitando contagens, agrupamentos, classificação por categorias, exames temporais e avaliação de padrões de consulta ao acervo.
-- MAGIC
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3) Modelagem (Conceitual)
-- MAGIC
-- MAGIC ### 3.1 Modelo Escolhido: Flat Table
-- MAGIC
-- MAGIC Para esta MVP, será adotado um modelo de dados baseado em **tabela única (flat table)**.  
-- MAGIC Esse modelo consiste em consolidar todas as colunas relevantes do dataset original em uma única tabela tratada, padronizada e pronta para análise.
-- MAGIC
-- MAGIC A escolha pelo modelo flat atende ao escopo de um pipeline mínimo viável, permitindo demonstrar todas as etapas essenciais (ingestão, transformação, carga e análise) sem a complexidade de um modelo multidimensional.
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC ### 3.2 Justificativa para o Uso de Tabela Única
-- MAGIC
-- MAGIC A decisão de utilizar um modelo flat é fundamentada em critérios práticos e conceituais:
-- MAGIC
-- MAGIC - O objetivo da MVP é demonstrar um pipeline operacional e não uma arquitetura avançada de Data Warehouse.  
-- MAGIC - O dataset possui estrutura já semi-normalizada e não apresenta granularidade suficiente para justificar múltiplas dimensões.  
-- MAGIC - O modelo flat permite realizar todas as análises necessárias utilizando apenas SQL.  
-- MAGIC - Reduz a complexidade e o tempo de implementação, mantendo o foco nos requisitos do projeto.  
-- MAGIC - Facilita o entendimento e a reprodução do fluxo por qualquer avaliador.
-- MAGIC
-- MAGIC O modelo flat, portanto, é **adequado**, **funcional** e ** totalmente alinhado ao escopo mínimo viável** exigido.
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC ### 3.3 Capricho: Possível Evolução para um Mini Data Warehouse (DW)
-- MAGIC
-- MAGIC Para fins didáticos e para demonstrar compreensão dos conceitos da disciplina de Data Warehouse, é possível descrever como este dataset poderia ser modelado em um esquema estrela (**sem implementação prática nesta MVP**).
-- MAGIC
-- MAGIC Um modelo hipotético poderia incluir:
-- MAGIC
-- MAGIC - **dim_material:** categorias de suporte (livro, manuscrito, periódico etc.)  
-- MAGIC - **dim_unidade_acervo:** unidades de preservação da FBN  
-- MAGIC - **dim_tempo:** datas normalizadas (ano, mês, dia) e horários  
-- MAGIC - **dim_estado_fisico:** condição de preservação do material  
-- MAGIC - **fato_consultas:** cada registro de consulta com medidas como contagem, duração estimada, etc.
-- MAGIC
-- MAGIC Embora não seja implementado aqui, este apontamento evidencia capacidade de planejar evoluções arquiteturais e demonstra domínio conceitual da área.
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC ### 3.4 Modelagem Conceitual Final da Tabela Tratada
-- MAGIC
-- MAGIC A tabela tratada final — que será construída posteriormente via **CTAS (CREATE TABLE AS SELECT)** durante o ETL — terá a seguinte **estrutura conceitual**:
-- MAGIC
-- MAGIC - **Colunas padronizadas** (nomes normalizados para formato SQL)  
-- MAGIC - **Dados limpos e formatados** (categorias padronizadas, remoção de nulos/duplicados quando aplicável)  
-- MAGIC - **Datas normalizadas** (padrão ISO yyyy-MM-dd sempre que possível)  
-- MAGIC - **Horários tratados** (ou mantidos como texto padronizado, conforme qualidade do dataset)  
-- MAGIC - **Criação de colunas derivadas simples**, como:  
-- MAGIC   - `ano_consulta` (extraído da data de início)  
-- MAGIC   - `tipo_material_normalizado`  
-- MAGIC   - `duracao_estimada` (opcional, caso qualidade dos horários permita)  
-- MAGIC
-- MAGIC A tabela final será materializada como:
-- MAGIC
-- MAGIC tb_fbn_consultas_tratada
-- MAGIC Essa tabela será a base de todas as consultas analíticas e visualizações produzidas nas etapas seguintes.
-- MAGIC
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4) ETL (Extract, Transform, Load)
-- MAGIC Esta etapa documenta o pipeline de ingestão, transformação e carga dos dados no ambiente Databricks.  
-- MAGIC A execução prática será evidenciada por blocos SQL e screenshots, conforme orientado no enunciado da MVP.
-- MAGIC
-- MAGIC ### 4.1 Extract
-- MAGIC
-- MAGIC **Ingestão do Arquivo CSV**<br>
-- MAGIC A etapa de ingestão consiste em carregar o arquivo CSV bruto para o Databricks, criando automaticamente uma tabela inicial (tabela bruta) armazenada na camada *Raw*.
-- MAGIC
-- MAGIC **Passos executados:**
-- MAGIC 1. Acessar: *Compute Workspace* → *Upload Data*  
-- MAGIC 2. Selecionar o arquivo CSV original obtido na etapa de coleta  
-- MAGIC 3. Confirmar criação automática da tabela  
-- MAGIC 4. Registrar o nome atribuído pelo Databricks à tabela bruta  
-- MAGIC
-- MAGIC **Evidências previstas:**
-- MAGIC - Screenshot do upload (“Upload Data”)  
-- MAGIC - Screenshot do preview da tabela bruta 
-- MAGIC - Nome final da tabela criada automaticamente
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC **Evidência — Ingestão (Upload e Criação da Tabela Bruta)**
-- MAGIC
-- MAGIC **Descrição:**  
-- MAGIC Criação da tabela `tb_surdo_bruta_utf8` a partir do CSV original carregado via interface do Databricks.
-- MAGIC
-- MAGIC ![Upload e Create Table](https://raw.githubusercontent.com/chascaldini/teste_mvp-databricks-acessibilidade/refs/heads/main/img/etapa-ingest-create-table.jpeg)
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC **Evidência — Inspeção Inicial da Tabela Bruta**
-- MAGIC
-- MAGIC **Descrição:**  
-- MAGIC Primeira consulta para visualizar a estrutura do arquivo carregado e conferir necessidade de limpeza.
-- MAGIC
-- MAGIC ![Inspeção inicial SELECT](https://raw.githubusercontent.com/chascaldini/teste_mvp-databricks-acessibilidade/refs/heads/main/img/etapa-ingest-inspecao-select.jpeg)
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC **Evidência — Sample Data (pré-transformação)**
-- MAGIC
-- MAGIC **Descrição:**  
-- MAGIC Visualização automática de amostras do Databricks (antes da etapa de tratamento). Usada apenas quando disponível.
-- MAGIC
-- MAGIC ![Sample Data](https://raw.githubusercontent.com/chascaldini/teste_mvp-databricks-acessibilidade/main/img/etapa-ingest-sample-data.jpeg)
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC **Comando de inspeção realizado após a ingestão:**
-- MAGIC
-- MAGIC ```sql
-- MAGIC SELECT * FROM nome_da_tabela_bruta
-- MAGIC LIMIT 20;
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Evidência — Sample Data (pré-transformação)**
-- MAGIC
-- MAGIC **Descrição:**  
-- MAGIC Visualização automática de amostras do Databricks (antes da etapa de tratamento). Usada apenas quando disponível.
-- MAGIC
-- MAGIC ![Sample Data](https://raw.githubusercontent.com/chascaldini/teste_mvp-databricks-acessibilidade/main/img/etapa-ingest-sample-data.jpeg)
-- MAGIC

-- COMMAND ----------

SELECT * FROM tb_surdo_bruta_utf8 LIMIT 15;


-- COMMAND ----------

DESCRIBE TABLE tb_surdo_bruta

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 4.2 Transform
-- MAGIC
-- MAGIC **Inspeção Inicial da Tabela Bruta**<br>
-- MAGIC Antes de aplicar as transformações, realizei uma inspeção estrutural e visual da tabela `tb_surdo_bruta_utf8`, verificando:
-- MAGIC
-- MAGIC - nomes e tipos das colunas (`DESCRIBE`)
-- MAGIC - amostra de linhas para checar encoding, acentuação e consistência dos campos (`SELECT * LIMIT 20`)
-- MAGIC
-- MAGIC Essas consultas orientam as decisões de limpeza e padronização que serão aplicadas nas próximas etapas.
-- MAGIC
-- MAGIC **Limpeza e Padronização**
-- MAGIC
-- MAGIC A transformação é realizada inteiramente via SQL, sem alterar a tabela bruta.  
-- MAGIC As operações aplicadas incluem:
-- MAGIC
-- MAGIC - limpeza de linhas inconsistentes ou incompletas  
-- MAGIC - normalização de valores textuais  
-- MAGIC - padronização de nomes de colunas  
-- MAGIC - criação de colunas derivadas simples  
-- MAGIC - tratamento básico de datas e horários  
-- MAGIC - remoção de duplicidades (se necessário)
-- MAGIC
-- MAGIC **Evidências previstas:**
-- MAGIC - Blocos SQL contendo cada transformação aplicada  
-- MAGIC - Explicação em Markdown (em linguagem natural) justificando cada operação  
-- MAGIC - Exemplo de *before/after* quando aplicável
-- MAGIC

-- COMMAND ----------

SELECT *
FROM tb_surdo_bruta_utf8 
LIMIT 50;


-- COMMAND ----------

DESCRIBE tb_surdo_bruta_utf8;


-- COMMAND ----------

DESCRIBE tb_surdo_bruta_utf8 ;

-- COMMAND ----------

SELECT
  split(`Nome da Base de Dados;Base específica;Disciplina;Curso;Campus;Ano`, ';')[0] AS nome_base,
  split(`Nome da Base de Dados;Base específica;Disciplina;Curso;Campus;Ano`, ';')[1] AS base_especifica,
  split(`Nome da Base de Dados;Base específica;Disciplina;Curso;Campus;Ano`, ';')[2] AS disciplina,
  split(`Nome da Base de Dados;Base específica;Disciplina;Curso;Campus;Ano`, ';')[3] AS curso,
  split(`Nome da Base de Dados;Base específica;Disciplina;Curso;Campus;Ano`, ';')[4] AS campus,
  split(`Nome da Base de Dados;Base específica;Disciplina;Curso;Campus;Ano`, ';')[5] AS ano
FROM tb_surdo_bruta_utf8 ;


-- COMMAND ----------

SELECT COUNT(*) AS total,
       COUNT(DISTINCT *) AS distintos
FROM tb_surdo_bruta_utf8;


-- COMMAND ----------

SELECT *
FROM tb_surdo_bruta_utf8
WHERE `Nome da Base de Dados;Base específica;Disciplina;Curso;Campus;Ano` IS NULL
   OR TRIM(`Nome da Base de Dados;Base específica;Disciplina;Curso;Campus;Ano`) = "";


-- COMMAND ----------

SELECT
  TRIM(split(`Nome da Base de Dados;Base específica;Disciplina;Curso;Campus;Ano`, ';')[0]) AS nome_base,
  TRIM(split(`Nome da Base de Dados;Base específica;Disciplina;Curso;Campus;Ano`, ';')[1]) AS base_especifica,
  TRIM(split(`Nome da Base de Dados;Base específica;Disciplina;Curso;Campus;Ano`, ';')[2]) AS disciplina,
  TRIM(split(`Nome da Base de Dados;Base específica;Disciplina;Curso;Campus;Ano`, ';')[3]) AS curso,
  TRIM(split(`Nome da Base de Dados;Base específica;Disciplina;Curso;Campus;Ano`, ';')[4]) AS campus,
  TRIM(split(`Nome da Base de Dados;Base específica;Disciplina;Curso;Campus;Ano`, ';')[5]) AS ano
FROM tb_surdo_bruta_utf8
LIMIT 20;


-- COMMAND ----------

SELECT COUNT(*) FROM tb_surdo_bruta_utf8;


-- COMMAND ----------

SELECT
  CASE 
      WHEN campus = 'Campus Benfica' THEN 'Benfica'
      WHEN campus = 'Campus do Pici' THEN 'Pici'
      ELSE campus
  END AS campus_normalizado
FROM tb_surdo_tratada;


-- COMMAND ----------

DESCRIBE tb_surdo_bruta_utf8;


-- COMMAND ----------

SELECT
  -- Separando e limpando as colunas
  TRIM(split(`Nome da Base de Dados;Base específica;Disciplina;Curso;Campus;Ano`, ';')[0]) AS nome_base,
  TRIM(split(`Nome da Base de Dados;Base específica;Disciplina;Curso;Campus;Ano`, ';')[1]) AS base_especifica,
  TRIM(split(`Nome da Base de Dados;Base específica;Disciplina;Curso;Campus;Ano`, ';')[2]) AS disciplina,
  TRIM(split(`Nome da Base de Dados;Base específica;Disciplina;Curso;Campus;Ano`, ';')[3]) AS curso,

  -- CASE para renomear valores de campus
  CASE 
      WHEN TRIM(split(`Nome da Base de Dados;Base específica;Disciplina;Curso;Campus;Ano`, ';')[4]) = 'Campus Benfica' THEN 'Benfica'
      WHEN TRIM(split(`Nome da Base de Dados;Base específica;Disciplina;Curso;Campus;Ano`, ';')[4]) = 'Campus do Pici' THEN 'Pici'
      ELSE TRIM(split(`Nome da Base de Dados;Base específica;Disciplina;Curso;Campus;Ano`, ';')[4])
  END AS campus,

  -- Ano, limpo
  TRIM(split(`Nome da Base de Dados;Base específica;Disciplina;Curso;Campus;Ano`, ';')[5]) AS ano

FROM tb_surdo_bruta_utf8;


-- COMMAND ----------

DROP TABLE tb_surdo_tratada;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 4.3 Load 
-- MAGIC
-- MAGIC **Criação da Tabela Final Tratada**<br>
-- MAGIC Após a carga, a tabela tb_surdo_tratada foi inspecionada para verificar sua estrutura e consistência.
-- MAGIC O preview das primeiras linhas (Figura X) mostra as colunas já separadas e padronizadas.
-- MAGIC O total de 72 registros foi mantido, e os valores de campus foram normalizados conforme a regra definida na etapa de Transform.%md
-- MAGIC
-- MAGIC Após a padronização dos dados, a tabela final tratada é criada utilizando o comando **CTAS (CREATE TABLE AS SELECT)**, que combina DDL e seleção em uma única operação, materializando o resultado da transformação.
-- MAGIC
-- MAGIC A tabela final recebe o nome:
-- MAGIC tb_fbn_consultas_tratada
-- MAGIC
-- MAGIC **Validação da Tabela Tratada**
-- MAGIC
-- MAGIC Após a execução do comando CTAS, a tabela tb_surdo_tratada foi inspecionada para confirmar sua estrutura e consistência.
-- MAGIC Os prints apresentados demonstram que:
-- MAGIC
-- MAGIC todas as colunas foram corretamente separadas a partir da coluna única original (via split);
-- MAGIC
-- MAGIC os textos foram padronizados com trim e regras de normalização;
-- MAGIC
-- MAGIC os valores da coluna campus foram harmonizados com a lógica CASE, garantindo consistência;
-- MAGIC
-- MAGIC o número total de linhas permanece inalterado em relação à tabela bruta, evidenciando que não houve perda nem duplicação de registros.
-- MAGIC
-- MAGIC Essas evidências confirmam que a etapa de Load foi bem-sucedida e que a tabela final está pronta para a fase de análise e visualização.
-- MAGIC
-- MAGIC
-- MAGIC **Comando SQL (exemplo):**
-- MAGIC
-- MAGIC ```sql
-- MAGIC CREATE TABLE tb_fbn_consultas_tratada AS
-- MAGIC SELECT 
-- MAGIC     -- colunas tratadas aqui
-- MAGIC FROM nome_da_tabela_bruta_tratada; 
-- MAGIC
-- MAGIC **Evidências previstas:**
-- MAGIC
-- MAGIC Bloco SQL contendo o CTAS
-- MAGIC - Screenshot do resultado (SELECT * LIMIT 20)
-- MAGIC - Exportação da tabela final em CSV para Power BI ou GitHub
-- MAGIC - Exportação em Parquet (capricho opcional)

-- COMMAND ----------

CREATE TABLE tb_surdo_tratada AS
SELECT
  TRIM(split(`Nome da Base de Dados;Base específica;Disciplina;Curso;Campus;Ano`, ';')[0]) AS nome_base,
  TRIM(split(`Nome da Base de Dados;Base específica;Disciplina;Curso;Campus;Ano`, ';')[1]) AS base_especifica,
  TRIM(split(`Nome da Base de Dados;Base específica;Disciplina;Curso;Campus;Ano`, ';')[2]) AS disciplina,
  TRIM(split(`Nome da Base de Dados;Base específica;Disciplina;Curso;Campus;Ano`, ';')[3]) AS curso,

  CASE 
      WHEN TRIM(split(`Nome da Base de Dados;Base específica;Disciplina;Curso;Campus;Ano`, ';')[4]) = 'Campus Benfica' THEN 'Benfica'
      WHEN TRIM(split(`Nome da Base de Dados;Base específica;Disciplina;Curso;Campus;Ano`, ';')[4]) = 'Campus do Pici' THEN 'Pici'
      ELSE TRIM(split(`Nome da Base de Dados;Base específica;Disciplina;Curso;Campus;Ano`, ';')[4])
  END AS campus,

  TRIM(split(`Nome da Base de Dados;Base específica;Disciplina;Curso;Campus;Ano`, ';')[5]) AS ano

FROM tb_surdo_bruta_utf8;


-- COMMAND ----------

SELECT * FROM tb_surdo_tratada LIMIT 20;


-- COMMAND ----------

DESCRIBE TABLE tb_surdo_tratada;


-- COMMAND ----------

SELECT COUNT(*) FROM tb_surdo_tratada

-- COMMAND ----------

-- Conferindo valores distintos da coluna campus
SELECT DISTINCT campus
FROM tb_surdo_tratada;


-- COMMAND ----------

-- Quantidade de registros por campus
SELECT 
  campus,
  COUNT(*) AS total
FROM tb_surdo_tratada
GROUP BY campus
ORDER BY total DESC;


-- COMMAND ----------

-- Quantidade de disciplinas distintas na base
SELECT 
  COUNT(DISTINCT disciplina) AS disciplinas_distintas
FROM tb_surdo_tratada;


-- COMMAND ----------

SELECT DISTINCT disciplina
FROM tb_surdo_tratada
ORDER BY disciplina;


-- COMMAND ----------

-- Quantidade de cursos distintos
SELECT 
  COUNT(DISTINCT curso) AS cursos_distintos
FROM tb_surdo_tratada;


-- COMMAND ----------

SELECT DISTINCT curso
FROM tb_surdo_tratada
ORDER BY curso;


-- COMMAND ----------

SELECT * FROM tb_surdo_tratada;


-- COMMAND ----------

CREATE OR REPLACE TABLE tb_surdo_tratada_parquet
USING PARQUET
AS
SELECT *
FROM tb_surdo_tratada;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5) Análise dos dados e Métricas
-- MAGIC - Objetivo da Análise – o que será analisado e por quê
-- MAGIC - Qualidade dos Dados – limpeza, nulos, duplicatas, consistência
-- MAGIC - Métricas – as consultas SQL que geram insights
-- MAGIC - Visualizações (opcional) – se houver
-- MAGIC - Discussão dos Resultados – interpretação dos números
-- MAGIC - Evidências – prints e outputs organizados
-- MAGIC
-- MAGIC ### 5.1 Objetivo da Análise
-- MAGIC Breve explicação do que a análise pretende verificar, vinculando ao objetivo do MVP.
-- MAGIC
-- MAGIC Nesta etapa, o objetivo é avaliar a qualidade da tabela tratada `tb_surdo_tratada` e, em seguida, produzir análises simples que respondam às perguntas definidas no objetivo do trabalho. A análise está focada em entender como os atendimentos à comunidade surda se distribuem por campus, curso e disciplina.
-- MAGIC
-- MAGIC Nesta etapa, o objetivo é analisar a tabela tratada `tb_surdo_tratada` para responder às perguntas definidas no planejamento do trabalho. A análise está focada em entender como os atendimentos à comunidade surda se distribuem entre campi, cursos e disciplinas, bem como verificar se o conjunto de dados apresenta qualidade suficiente para sustentar conclusões confiáveis.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 5.2 Qualidade dos Dados (Data Quality)
-- MAGIC Análise técnica da integridade:
-- MAGIC - nulos
-- MAGIC - duplicatas
-- MAGIC - inconsistências
-- MAGIC - cardinalidade dos atributos
-- MAGIC - coerência básica da estrutura
-- MAGIC //
-- MAGIC Nesta subseção foram executadas consultas em SQL para verificar a consistência da tabela tratada:
-- MAGIC
-- MAGIC - contagem total de registros;
-- MAGIC - verificação de valores nulos por coluna;
-- MAGIC - inspeção de valores distintos em campos categóricos (campus, curso, disciplina);
-- MAGIC - identificação de possíveis registros duplicados.
-- MAGIC
-- MAGIC A partir dessas consultas, foi possível avaliar se o conjunto de dados está adequado para a análise ou se havia problemas que poderiam comprometer as respostas às perguntas de negócio. Caso fossem identificados problemas relevantes, seriam propostos ajustes ou apontadas as limitações na discussão dos resultados.
-- MAGIC //
-- MAGIC
-- MAGIC Antes de explorar métricas e distribuições, foi realizada uma verificação da qualidade da tabela `tb_surdo_tratada`. Nessa verificação, foram executadas consultas em SQL para:
-- MAGIC
-- MAGIC - contar o número total de registros;
-- MAGIC - verificar a presença de valores nulos por coluna;
-- MAGIC - identificar possíveis registros duplicados;
-- MAGIC - inspecionar a variedade de valores em campos categóricos (campus, curso, disciplina).
-- MAGIC
-- MAGIC Os resultados indicaram que:
-- MAGIC
-- MAGIC - não foram encontrados valores nulos nas colunas analisadas;
-- MAGIC - não foram identificados registros duplicados com a combinação de campos utilizada;
-- MAGIC - o conjunto apresenta 72 disciplinas distintas, 12 cursos distintos e 2 campi distintos.
-- MAGIC
-- MAGIC Esses achados sugerem que a base está coerente e adequada para a etapa de análise, sem necessidade de correções adicionais além das transformações já aplicadas na fase de ETL. As consultas de verificação e seus respectivos outputs estão registradas nos blocos SQL desta subseção.
-- MAGIC

-- COMMAND ----------

-- conta nulos por coluna
SELECT
  SUM(CASE WHEN nome_base IS NULL OR trim(nome_base) = '' THEN 1 END) AS nulos_nome_base,
  SUM(CASE WHEN base_especifica IS NULL OR trim(base_especifica) = '' THEN 1 END) AS nulos_base_especifica,
  SUM(CASE WHEN disciplina IS NULL OR trim(disciplina) = '' THEN 1 END) AS nulos_disciplina,
  SUM(CASE WHEN curso IS NULL OR trim(curso) = '' THEN 1 END) AS nulos_curso,
  SUM(CASE WHEN campus IS NULL OR trim(campus) = '' THEN 1 END) AS nulos_campus,
  SUM(CASE WHEN ano IS NULL OR trim(ano) = '' THEN 1 END) AS nulos_ano
FROM tb_surdo_tratada;


-- COMMAND ----------

-- valores distintos por coluna
SELECT
  COUNT(DISTINCT disciplina) AS disciplinas_distintas,
  COUNT(DISTINCT curso) AS cursos_distintos,
  COUNT(DISTINCT campus) AS campus_distintos
FROM tb_surdo_tratada;


-- COMMAND ----------

-- procurando duplicatas
SELECT nome_base, disciplina, curso, campus, ano, COUNT(*) AS qtd
FROM tb_surdo_tratada
GROUP BY nome_base, disciplina, curso, campus, ano
HAVING COUNT(*) > 1;


-- COMMAND ----------

-- registros totais
SELECT COUNT(*) FROM tb_surdo_tratada;


-- COMMAND ----------

-- frequencia por categoria 
SELECT curso, COUNT(*) AS total
FROM tb_surdo_tratada
GROUP BY curso
ORDER BY total DESC;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 5.3 Resultado da Análise de Qualidade
-- MAGIC
-- MAGIC A avaliação da qualidade dos dados da tabela tb_surdo_tratada indicou que:
-- MAGIC Não há valores nulos em nenhuma coluna relevante.
-- MAGIC Não há registros duplicados, conforme verificação por combinação de campos-chave.
-- MAGIC Os campos categóricos apresentam:
-- MAGIC 72 disciplinas distintas
-- MAGIC 12 cursos distintos
-- MAGIC 2 campi distintos (“Benfica” e “Pici”)
-- MAGIC
-- MAGIC Esses resultados indicam que o conjunto de dados está consistente, bem estruturado e adequado para a análise, sem necessidade de correções adicionais além das transformações realizadas na etapa anterior.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC | Coluna        | Nulos | Observação            |
-- MAGIC |---------------|-------|-----------------------|
-- MAGIC | nome_base     | 0     | Sem problemas         |
-- MAGIC | curso         | 0     | Sem problemas         |
-- MAGIC | campus        | 0     | Sem problemas         |
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC | Coluna            | Nulos | Distintos | Observação               |
-- MAGIC |-------------------|-------|-----------|---------------------------|
-- MAGIC | nome_base         | 0     | 1         | Valor único               |
-- MAGIC | base_especifica   | 0     | 1         | Sem variação              |
-- MAGIC | disciplina        | 0     | 72        | Alta cardinalidade        |
-- MAGIC | curso             | 0     | 12        | Distribuição moderada     |
-- MAGIC | campus            | 0     | 2         | “Benfica” e “Pici”        |
-- MAGIC | ano               | 0     | 1         | Valor único               |
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 5.4 Análises Exploratórias e Métricas (Analytics)
-- MAGIC Consultas SQL para entender padrões, tais como:
-- MAGIC - distribuição por campus
-- MAGIC - distribuição por curso
-- MAGIC - distribuição por disciplina
-- MAGIC - percentuais por categoria
-- MAGIC - rankings
-- MAGIC //
-- MAGIC Depois de verificada a qualidade dos dados, foram executadas consultas de agregação (`GROUP BY`) para explorar o comportamento do conjunto de dados, tais como:
-- MAGIC
-- MAGIC - quantidade de registros por campus;
-- MAGIC - quantidade de registros por curso;
-- MAGIC - distribuição de disciplinas atendidas;
-- MAGIC - outras combinações simples consideradas relevantes.
-- MAGIC
-- MAGIC Essas consultas permitem observar como os atendimentos se distribuem no contexto analisado e fornecem insumos para responder às perguntas formuladas na etapa de planejamento.
-- MAGIC //
-- MAGIC
-- MAGIC Com a qualidade dos dados verificada, foram executadas consultas exploratórias em SQL para observar padrões de distribuição e responder às perguntas de análise propostas. As principais métricas investigadas foram:
-- MAGIC
-- MAGIC - quantidade de registros por campus;
-- MAGIC - quantidade de registros por curso;
-- MAGIC - quantidade de registros por disciplina;
-- MAGIC - ranking dos cursos com maior número de registros;
-- MAGIC - percentual de participação de cada campus no total de atendimentos.
-- MAGIC
-- MAGIC Essas consultas foram implementadas com comandos `GROUP BY` e funções de agregação (`COUNT`, `ROUND`), permitindo uma visão sintética do comportamento dos dados. Os resultados numéricos dessas consultas, apresentados em forma de tabelas, servem de base para a interpretação dos padrões observados e para a discussão na subseção seguinte.

-- COMMAND ----------

-- Distribuição por Campus
SELECT 
  campus,
  COUNT(*) AS total_registros
FROM tb_surdo_tratada
GROUP BY campus
ORDER BY total_registros DESC;


-- COMMAND ----------

-- Distribuição por Curso
SELECT 
  curso,
  COUNT(*) AS total_registros
FROM tb_surdo_tratada
GROUP BY curso
ORDER BY total_registros DESC;


-- COMMAND ----------

-- distribuição por disciplina
SELECT 
  disciplina,
  COUNT(*) AS total_registros
FROM tb_surdo_tratada
GROUP BY disciplina
ORDER BY total_registros DESC;


-- COMMAND ----------

-- top 5 cursos com mais atendimento
SELECT 
  curso,
  COUNT(*) AS total_registros
FROM tb_surdo_tratada
GROUP BY curso
ORDER BY total_registros DESC
LIMIT 5;


-- COMMAND ----------

-- percentual por campus
SELECT
  campus,
  COUNT(*) AS total_registros,
  ROUND(
    COUNT(*) * 100.0 / (SELECT COUNT(*) FROM tb_surdo_tratada),
    2
  ) AS percentual
FROM tb_surdo_tratada
GROUP BY campus
ORDER BY percentual DESC;


-- COMMAND ----------

-- quantidade de disciplinas por campus
SELECT
  campus,
  COUNT(DISTINCT disciplina) AS disciplinas_distintas
FROM tb_surdo_tratada
GROUP BY campus
ORDER BY disciplinas_distintas DESC;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 5.5 Visualizações e Dashboard
-- MAGIC Caso utilize:
-- MAGIC - gráficos internos do Databricks  
-- MAGIC ou  
-- MAGIC - prints do Power BI
-- MAGIC //
-- MAGIC Quando pertinente, os resultados das consultas foram transformados em visualizações simples:
-- MAGIC
-- MAGIC - gráficos de barras ou colunas (por exemplo, atendimentos por campus ou por curso);
-- MAGIC - construção de gráficos diretamente no Databricks ou, opcionalmente, em ferramenta externa (como Power BI), a partir do CSV da tabela tratada.
-- MAGIC
-- MAGIC As visualizações são utilizadas apenas como apoio à interpretação e não substituem as consultas em SQL, que permanecem documentadas no notebook.
-- MAGIC //
-- MAGIC
-- MAGIC A partir dos resultados das consultas exploratórias, algumas métricas foram representadas por meio de visualizações simples, como gráficos de barras ou colunas (por exemplo, atendimentos por campus ou por curso). Esses gráficos podem ser produzidos diretamente no Databricks, a partir dos resultados das consultas, ou em ferramenta externa de BI (como Power BI), utilizando o CSV da tabela tratada.
-- MAGIC
-- MAGIC As visualizações têm caráter complementar, servindo como apoio visual à interpretação das distribuições e facilitando a comunicação dos resultados. As imagens ou prints dos gráficos gerados estão anexados junto aos resultados das consultas correspondentes.
-- MAGIC
-- MAGIC ---

-- COMMAND ----------

SELECT campus, COUNT(*) AS total
FROM tb_surdo_tratada
GROUP BY campus;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 5.6 Discussão dos Resultados
-- MAGIC Texto interpretando as métricas:
-- MAGIC - o que os números significam
-- MAGIC - padrões observados
-- MAGIC - hipóteses
-- MAGIC - limitações da análise
-- MAGIC - o que responde à pergunta do MVP
-- MAGIC //
-- MAGIC Com base nas consultas e visualizações, foi produzida uma breve discussão em linguagem natural, conectando:
-- MAGIC
-- MAGIC - os números observados;
-- MAGIC - as perguntas de negócio definidas no objetivo;
-- MAGIC - as possíveis interpretações e limitações.
-- MAGIC
-- MAGIC Nesta seção também são registradas eventuais limitações do conjunto de dados (falta de granularidade, ausência de variáveis importantes, concentração em poucos campi ou cursos, etc.) e sugestões de como a análise poderia ser aprimorada em versões futuras do trabalho.
-- MAGIC
-- MAGIC Resultados e Discussão (integração final da análise)  
-- MAGIC Retoma as métricas principais dos analytics  
-- MAGIC Comenta padrões, tendências, hipóteses e possíveis explicações  
-- MAGIC Liga os resultados ao objetivo do MVP  
-- MAGIC Comenta limitações da análise  
-- MAGIC
-- MAGIC ///
-- MAGIC
-- MAGIC Com base nas métricas e visualizações obtidas, é possível destacar alguns pontos:
-- MAGIC
-- MAGIC - o campus com maior número de registros concentra a maior parte dos atendimentos, sugerindo uma demanda mais intensa nesse local para apoio à comunidade surda;
-- MAGIC - o outro campus apresenta um volume menor de registros, mas relacionado a um conjunto específico de cursos e disciplinas;
-- MAGIC - a diversidade de disciplinas e cursos atendidos indica que o serviço de tradução/intérprete de Libras está distribuído por diferentes áreas de conhecimento, e não restrito a um único tipo de componente curricular.
-- MAGIC
-- MAGIC Esses resultados ajudam a responder à pergunta central do trabalho, oferecendo uma visão clara de como os atendimentos se distribuem no contexto analisado. Ao mesmo tempo, algumas limitações permanecem, como a ausência de variáveis mais detalhadas sobre perfil dos estudantes, modalidades de atendimento ou indicadores temporais mais granulares, que poderiam enriquecer análises futuras.
-- MAGIC
-- MAGIC
-- MAGIC ### 5.7 Evidências da Análise
-- MAGIC “As evidências desta etapa estão documentadas nos blocos SQL, tabelas e gráficos deste notebook, incluindo: …”
-- MAGIC Lista objetiva do que foi comprovado:
-- MAGIC - prints dos SQL
-- MAGIC - outputs
-- MAGIC - gráficos
-- MAGIC - resumo do que foi encontrado
-- MAGIC //
-- MAGIC
-- MAGIC As evidências desta etapa incluem:
-- MAGIC
-- MAGIC - consultas SQL de verificação da qualidade dos dados (contagem de registros, nulos, duplicatas e valores distintos);
-- MAGIC - consultas SQL de análise exploratória e cálculo de métricas (distribuição por campus, curso e disciplina, percentuais e rankings);
-- MAGIC - tabelas de resultados exibidas no notebook;
-- MAGIC - gráficos gerados a partir dessas consultas, quando aplicável.
-- MAGIC
-- MAGIC Esses elementos estão documentados ao longo do notebook e podem ser consultados em conjunto com o texto desta seção para acompanhar a análise passo a passo.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 6) Governança, Linhagem e Considerações Éticas
-- MAGIC
-- MAGIC ### 6.1  Catálogo de Dados (Data Dictionary)
-- MAGIC Tabela Markdown contendo:
-- MAGIC Campo	Tipo	Descrição	Origem	Observações
-- MAGIC Serve para provar que você entendeu os campos resultantes da tabela tratada.
-- MAGIC
-- MAGIC **capricho:** marcar colunas derivadas com tag, por exemplo no campo Origem:
-- MAGIC Derivada no Transform (SQL)  
-- MAGIC Normalizada no Transform  
-- MAGIC Criada via CASE  
-- MAGIC Criada via SPLIT/TRIM  
-- MAGIC Derivada no Transform (CASE WHEN)
-- MAGIC Derivada no Transform (SPLIT + TRIM)
-- MAGIC Criada via normalização
-- MAGIC Criada manualmente no ETL
-- MAGIC Renomeada no Transform
-- MAGIC SPLIT TRIM CASE REGEXP UPPER/LOWER REPLACE
-- MAGIC
-- MAGIC Campo derivado no processo ETL
-- MAGIC Exemplo:  
-- MAGIC Origem: Derivada no Transform (SPLIT + TRIM)
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 6.1 Catálogo de Dados (Data Dictionary)
-- MAGIC
-- MAGIC A tabela a seguir documenta os principais campos da tabela tratada `tb_surdo_tratada`, descrevendo significado, tipo, origem e domínio de valores. Os campos estão organizados em grupos semânticos para facilitar a leitura.
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC #### Grupo 1 — Identificação da Base
-- MAGIC
-- MAGIC | Campo        | Tipo   | Descrição                                                   | Origem         | Exemplo                                   | Domínio / Notas                                          | Steward  |
-- MAGIC |--------------|--------|-------------------------------------------------------------|----------------|-------------------------------------------|----------------------------------------------------------|----------|
-- MAGIC | base_dados   | string | Nome da base ou projeto de atendimento registrado na fonte  | CSV original   | Atendimento à comunidade surda em sala    | Neste dataset específico, tende a ter um único valor     | Charlyne |
-- MAGIC | base_especifica | string | Categoria interna da base de dados (ex.: nível, tipo etc.) | CSV original | Disciplinas da graduação                  | Representa a subcategoria dentro da base principal       | Charlyne |
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC #### Grupo 2 — Informações Acadêmicas
-- MAGIC
-- MAGIC | Campo       | Tipo   | Descrição                                                   | Origem         | Exemplo                               | Domínio / Notas                                       | Steward  |
-- MAGIC |-------------|--------|-------------------------------------------------------------|----------------|---------------------------------------|--------------------------------------------------------|----------|
-- MAGIC | disciplina  | string | Nome da disciplina atendida pela equipe de intérpretes      | CSV original   | Psicologia e Educação de Surdos       | Texto livre; múltiplas disciplinas por curso           | Charlyne |
-- MAGIC | curso       | string | Curso ao qual a disciplina pertence                          | CSV original   | Letras Libras                         | Pequeno conjunto de cursos (ex.: Letras Libras, Eng. de Pesca) | Charlyne |
-- MAGIC | ano_periodo | string | Ano/período letivo em que a disciplina é ofertada           | CSV original   | 2025.1                                | Formato textual (ex.: AAAA.1, AAAA.2)                  | Charlyne |
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC #### Grupo 3 — Localização
-- MAGIC
-- MAGIC | Campo   | Tipo   | Descrição                                  | Origem         | Exemplo         | Domínio / Notas                           | Steward  |
-- MAGIC |---------|--------|--------------------------------------------|----------------|-----------------|-------------------------------------------|----------|
-- MAGIC | campus  | string | Campus da instituição onde ocorre a oferta | CSV original   | Campus Benfica  | Valores textuais completos                | Charlyne |
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC #### Grupo 4 — Campos Derivados (Transform / Normalização)
-- MAGIC
-- MAGIC > Campos criados ou normalizados durante a etapa **Transform** do ETL. Estes campos não existem no CSV original e são resultado de operações SQL (`SPLIT`, `TRIM`, `CASE`, etc.).
-- MAGIC
-- MAGIC | Campo         | Tipo   | Descrição                                          | Origem                                     | Exemplo  | Domínio / Notas                                  | Steward  |
-- MAGIC |---------------|--------|----------------------------------------------------|--------------------------------------------|----------|--------------------------------------------------|----------|
-- MAGIC | campus_norm   | string | Nome do campus padronizado para análise           | Derivada no Transform (`CASE WHEN`)        | Benfica  | Valores esperados: `Benfica`, `Pici`            | Charlyne |
-- MAGIC | curso_norm    | string | Versão normalizada do nome do curso (opcional)    | Derivada no Transform (`TRIM` / `UPPER`)   | LETRAS LIBRAS | Utilizado para padronizar possíveis variações de escrita | Charlyne |
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC ### Observações gerais do catálogo
-- MAGIC
-- MAGIC - **Origem `CSV original`** indica campos presentes diretamente no arquivo de dados aberto.  
-- MAGIC - **Origem `Derivada no Transform`** indica campos criados na etapa de transformação (ETL) via SQL, como parte do pipeline de limpeza e padronização.  
-- MAGIC - O campo **`campus_norm`**, por exemplo, é resultado de uma regra de negócio aplicada na transformação:
-- MAGIC
-- MAGIC   ```sql
-- MAGIC   CASE 
-- MAGIC       WHEN campus = 'Campus Benfica' THEN 'Benfica'
-- MAGIC       WHEN campus = 'Campus do Pici' THEN 'Pici'
-- MAGIC       ELSE campus
-- MAGIC   END AS campus_norm
-- MAGIC
-- MAGIC - O steward lógico dos dados, para fins deste MVP, é registrado como Charlyne, responsável pela curadoria, transformação e documentação da tabela tratada.
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC | Campo        | Tipo   | Descrição                                               | Origem        | Exemplo             | Domínio / Notas                                       | Steward   |
-- MAGIC |--------------|--------|---------------------------------------------------------|--------------|---------------------|-------------------------------------------------------|-----------|
-- MAGIC | base_dados   | string | Nome da base de dados ou projeto atendido              | CSV original | Atendimento à comunidade surda em sala | Único valor neste dataset específico                | Charlyne  |
-- MAGIC | curso        | string | Nome do curso em que a disciplina é ofertada           | CSV original | Letras Libras       | Poucas categorias (Letras Libras, Eng. de Pesca etc.) | Charlyne  |
-- MAGIC | campus       | string | Campus onde foi realizado o atendimento                 | Derivada SQL | Benfica             | Valores esperados: Benfica, Pici                      | Charlyne  |
-- MAGIC | ano_periodo  | string | Ano/semestre da oferta da disciplina                   | CSV original | 2025.1              | Formato textual, ex.: 2025.1                          | Charlyne  |
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Campos Derivados (Transform)
-- MAGIC
-- MAGIC | Campo          | Tipo   | Descrição                                 | Origem                      | Exemplo  | Domínio / Notas                      | Steward |
-- MAGIC |----------------|--------|-------------------------------------------|-----------------------------|----------|--------------------------------------|---------|
-- MAGIC | campus_norm    | string | Valor padronizado do campus               | Derivada no Transform (CASE) | Pici     | Benfica / Pici                       | Charlyne |
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 6.2 Linhagem dos Dados (Data Lineage + ASCII diagram)
-- MAGIC Texto explicando o fluxo Fonte → Coleta → Transform → Tabela tratada → Análise/BI
-- MAGIC Diagrama ASCII simples
-- MAGIC
-- MAGIC ## 6.2 Linhagem dos Dados (Data Lineage)
-- MAGIC
-- MAGIC A linhagem dos dados descreve o percurso completo do conjunto de dados desde sua origem até o produto final utilizado para análise. Este processo inclui as etapas de coleta, ingestão, transformação, carga e análise.
-- MAGIC
-- MAGIC 1. **Fonte Original (CSV)**  
-- MAGIC    Os dados foram obtidos no portal Dados Abertos do Governo Federal, disponibilizados no formato CSV, contendo informações sobre disciplinas atendidas por intérpretes de Libras.
-- MAGIC
-- MAGIC 2. **Coleta e Armazenamento na Nuvem (Ingestão)**  
-- MAGIC    O arquivo CSV foi carregado manualmente para o Databricks Community Edition por meio da ferramenta *Upload Data*, gerando a tabela bruta `tb_surdo_bruta_utf8`.
-- MAGIC
-- MAGIC 3. **Transformação (ETL – Transform)**  
-- MAGIC    A tabela bruta passou por operações de limpeza e padronização via SQL:  
-- MAGIC    - divisão da coluna única em múltiplas colunas (split + trim)  
-- MAGIC    - padronização de valores textuais (ex.: normalização dos campi)  
-- MAGIC    - remoção de linhas inválidas  
-- MAGIC    - criação de colunas derivadas  
-- MAGIC
-- MAGIC 4. **Carga da Tabela Final (CTAS – Create Table As Select)**  
-- MAGIC    As transformações foram materializadas na tabela final `tb_surdo_tratada` utilizando CTAS, que cria e popula a tabela em um único comando SQL.
-- MAGIC
-- MAGIC 5. **Análise e Visualização**  
-- MAGIC    A tabela tratada alimenta consultas SQL para análise exploratória, cálculos estatísticos e geração de gráficos no Databricks ou Power BI.
-- MAGIC
-- MAGIC -- O que NÃO pode faltar
-- MAGIC
-- MAGIC ✔ Nome da fonte
-- MAGIC ✔ Nome da tabela bruta
-- MAGIC ✔ Operações de transformação (pelo menos 4 delas)
-- MAGIC ✔ Nome da tabela tratada
-- MAGIC ✔ Onde foram feitas as análises
-- MAGIC ✔ Fluxo visual (ASCII)
-- MAGIC
-- MAGIC -- mais compacto:  
-- MAGIC CSV (gov.br)  
-- MAGIC       ↓ ingestão  
-- MAGIC tb_surdo_bruta_utf8  
-- MAGIC       ↓ transformação SQL  
-- MAGIC tb_surdo_tratada  
-- MAGIC       ↓ análises  
-- MAGIC consultas + gráficos  
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Diagrama da Linhagem dos Dados (ASCII)
-- MAGIC
-- MAGIC                         +---------------------------+
-- MAGIC                         |   Dados Abertos (CSV)     |
-- MAGIC                         |  Fonte Original - gov.br  |
-- MAGIC                         +-------------+-------------+
-- MAGIC                                       |
-- MAGIC                                       v
-- MAGIC                         +---------------------------+
-- MAGIC                         |  Ingestão no Databricks    |
-- MAGIC                         |  Tabela Bruta: tb_surdo_bruta_utf8 |
-- MAGIC                         +-------------+-------------+
-- MAGIC                                       |
-- MAGIC                                       v
-- MAGIC                         +---------------------------+
-- MAGIC                         |   Transformação (SQL)     |
-- MAGIC                         |  split, trim, clean, case |
-- MAGIC                         +-------------+-------------+
-- MAGIC                                       |
-- MAGIC                                       v
-- MAGIC                   +-------------------------------------------+
-- MAGIC                   |   Tabela Final Tratada (CTAS)             |
-- MAGIC                   |           tb_surdo_tratada                |
-- MAGIC                   +-------------+-----------------------------+
-- MAGIC                                       |
-- MAGIC                                       v
-- MAGIC                        +-------------------------------+
-- MAGIC                        |   Análises e Visualizações    |
-- MAGIC                        |  SQL + Gráficos Databricks    |
-- MAGIC                        +-------------------------------+
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Resumo da Linhagem**  
-- MAGIC Fonte (CSV) → Ingestão (Databricks) → Transform (SQL) → Tabela Tratada → Análise → Visualizações
-- MAGIC
-- MAGIC **Legenda:**
-- MAGIC - RAW = dados brutos
-- MAGIC - CURATED = dados tratados
-- MAGIC - ANALYTICS = tabela final usada na análise
-- MAGIC
-- MAGIC exemplos de formatação:
-- MAGIC
-- MAGIC code style:
-- MAGIC `tb_surdo_tratada`
-- MAGIC `Derivada no Transform`
-- MAGIC `Campus Benfica`
-- MAGIC
-- MAGIC Caixas (blockquote):
-- MAGIC > Campo derivado criado na etapa Transform
-- MAGIC
-- MAGIC ex:
-- MAGIC > **Campo derivado** criado na etapa Transform usando `CASE WHEN`.
-- MAGIC
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 6.3 Governança e Segurança de Dados
-- MAGIC
-- MAGIC A governança adotada neste MVP segue princípios básicos, adequados ao escopo introdutório do projeto, garantindo organização, rastreabilidade e consistência ao longo do pipeline.
-- MAGIC
-- MAGIC ### Práticas de Governança Aplicadas
-- MAGIC - **Manutenção da tabela bruta (Raw) sem alterações**  
-- MAGIC   Garante rastreabilidade e auditabilidade das etapas do ETL.
-- MAGIC
-- MAGIC - **Documentação completa das transformações**  
-- MAGIC   Todo o processo de limpeza, normalização e criação da tabela tratada foi descrito via SQL e Markdown.
-- MAGIC
-- MAGIC - **Nomenclatura padronizada para tabelas e colunas**  
-- MAGIC   Facilita reuso, leitura e integração com ferramentas externas.
-- MAGIC
-- MAGIC - **Catalogação dos dados**  
-- MAGIC   O Catálogo de Dados lista cada campo da tabela tratada, seus tipos, origem e significado.
-- MAGIC
-- MAGIC - **Linhagem de dados registrada em diagrama ASCII**  
-- MAGIC   Torna explícitas as etapas de ingestão, transformação e geração das saídas.
-- MAGIC
-- MAGIC ### Segurança de Dados
-- MAGIC Como os dados utilizados são públicos e não contêm informações pessoais:
-- MAGIC - **não há necessidade de anonimização ou criptografia**,  
-- MAGIC - **não há restrição legal quanto ao compartilhamento do notebook ou dos arquivos derivados**,  
-- MAGIC - **as práticas de segurança concentram-se apenas na organização e no bom uso da plataforma**, como:
-- MAGIC   - evitar sobrescrever tabelas acidentalmente,  
-- MAGIC   - manter as etapas separadas (Raw → Transform → Treated),  
-- MAGIC   - não incluir dados externos sensíveis no workspace.
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC Esses pontos atendem plenamente ao nível de governança e segurança esperado para um MVP de pipeline em ambiente acadêmico. 
-- MAGIC Governança básica: 
-- MAGIC imutabilidade da tabela bruta
-- MAGIC padronização da tratada
-- MAGIC dados públicos
-- MAGIC segurança mínima (ex.: acesso controlado)
-- MAGIC
-- MAGIC Segurança:
-- MAGIC O dataset é público.
-- MAGIC Não contém dados pessoais identificáveis.
-- MAGIC A tabela bruta foi mantida intacta (princípio da imutabilidade).
-- MAGIC A tabela tratada segue padronização de colunas e nomenclaturas.
-- MAGIC Se fosse um ambiente corporativo, haveria: controle de acesso por usuário, versionamento de tabelas, logs de transformação
-- MAGIC
-- MAGIC ### 6.4 Considerações de LGPD e Ética
-- MAGIC
-- MAGIC Os dados utilizados neste projeto são provenientes de bases públicas disponibilizadas oficialmente no portal **dados.gov.br**, atendendo aos princípios de transparência e reutilização previstos na Política de Dados Abertos do Governo Federal. Por se tratar de informações institucionais e agregadas — referentes a disciplinas, cursos e unidades acadêmicas — **não há qualquer dado pessoal identificável ou sensível**, conforme definido pela Lei Geral de Proteção de Dados Pessoais (LGPD – Lei nº 13.709/2018).
-- MAGIC
-- MAGIC Ainda assim, o projeto adota boas práticas de ética e governança ao:
-- MAGIC
-- MAGIC - preservar integralmente a integridade do arquivo bruto (camada *Raw*), sem alterações;
-- MAGIC - documentar todas as transformações aplicadas no pipeline ETL;
-- MAGIC - garantir que nenhuma análise permita reidentificação de indivíduos;
-- MAGIC - utilizar exclusivamente dados com licenças abertas e autorização explícita de uso.
-- MAGIC
-- MAGIC Portanto, este MVP opera em conformidade com a LGPD e segue os princípios de **minimização de dados, transparência, rastreabilidade e finalidade legítima**.
-- MAGIC
-- MAGIC ### 6.5 Conclusões Finais
-- MAGIC
-- MAGIC Este MVP demonstrou a construção de um pipeline de dados simples, porém completo, utilizando o Databricks Community Edition. A partir de um dataset público, foi possível implementar as etapas essenciais de ingestão, transformação, análise e documentação — consolidando uma tabela tratada adequada para consultas e visualizações.
-- MAGIC
-- MAGIC As principais conclusões incluem:
-- MAGIC
-- MAGIC - **O pipeline ETL funcionou conforme planejado**, com separação clara entre dado bruto, transformações e tabela final.
-- MAGIC - **Os dados da instituição analisada apresentaram boa qualidade geral**, com ausência de nulos e poucas inconsistências.
-- MAGIC - **As métricas revelaram padrões relevantes**, como distribuição das disciplinas e cursos atendidos.
-- MAGIC - **A documentação estruturada (catálogo, linhagem, governança)** reforçou a rastreabilidade e clareza do processo.
-- MAGIC - **O uso do Databricks se mostrou adequado** ao escopo da MVP, permitindo integração entre SQL, visualizações e Markdown.
-- MAGIC
-- MAGIC O projeto cumpriu todos os requisitos essenciais solicitados na disciplina, representando um “arroz com feijão bem feito”, sólido e replicável. Em versões futuras, seria possível ampliar o escopo incluindo mais variáveis, integrar múltiplos datasets ou evoluir o modelo para um pequeno Data Warehouse.
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 7) Autoavaliação
-- MAGIC
-- MAGIC A realização deste MVP permitiu uma compreensão prática do processo completo de tratamento e análise de dados em nuvem, desde a ingestão até a documentação final. 
-- MAGIC
-- MAGIC **O que consegui realizar:**  
-- MAGIC - implementei todo o pipeline ETL no Databricks usando SQL;  
-- MAGIC - desenvolvi uma tabela tratada a partir do dataset bruto;  
-- MAGIC - realizei análises de qualidade e métricas, com queries claras e replicáveis;  
-- MAGIC - produzi visualizações simples que apoiam a interpretação dos resultados;  
-- MAGIC - organizei o notebook com documentação completa, seguindo as recomendações da disciplina.
-- MAGIC
-- MAGIC **Desafios:**  
-- MAGIC O ponto mais desafiador foi a etapa de Transform, especialmente o entendimento inicial sobre encoding, normalização de colunas e criação da tabela tratada via CTAS. Com prática, o processo tornou-se mais intuitivo.
-- MAGIC
-- MAGIC **Aprendizados:**  
-- MAGIC Aprendi na prática o fluxo de um pipeline moderno, a importância da revisão de qualidade dos dados e o papel da documentação estruturada (catálogo, linhagem, governança). Também desenvolvi mais segurança no uso do Databricks e na criação de consultas SQL.
-- MAGIC
-- MAGIC **O que faria diferente em uma próxima versão:**  
-- MAGIC Ampliaria o escopo analítico, criaria métricas adicionais, adicionaria uma pequena tabela dimensional e exploraria visualizações mais ricas em Power BI.
-- MAGIC
-- MAGIC Este MVP representa meu **“arroz com feijão bem feito”**, atendendo aos requisitos essenciais, com clareza, boa documentação e coerência metodológica.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 8) Reprodutibilidade e Entrega
-- MAGIC - evidencias finais (o que está incluído no notebook)
-- MAGIC - Orientações para Reprodutibilidade
-- MAGIC código no GitHub
-- MAGIC prints
-- MAGIC reprodutibilidade

-- COMMAND ----------

-- MAGIC %md
-- MAGIC