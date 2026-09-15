-- Teste de sanidade
-- Volta 5293/5215/3494
SELECT ano, COUNT(*) AS total 
fROM unificados 
GROUP BY ano 
ORDER BY ano;

SHOW COLUMNS FROM vw;

-- Criar o banco da Gold
CREATE DATABASE IF NOT EXISTS db_gold

-- Configuração inicial
-- Criando a tag Top Tier:
-- Indicador criado para essas análises: vale 1 para profissionais que declararam sálario mensal de R$ 12.001 ou mais (faixas de R$ 12.001 até “Acima de R$ 40.001) e 0 para os demais. 
CREATE OR REPLACE VIEW vw AS
SELECT *,
  CASE
    WHEN ano = '2025' AND atuacao IN ('0', '1', 'Nao_informado') THEN 'Gestor'
    WHEN ano = '2023' AND atuacao = 'Nao_informado' THEN 'Análise de Dados'
    ELSE atuacao
  END AS atuacao_ok,
  CASE WHEN faixa_salarial LIKE '%12.001%' OR faixa_salarial LIKE '%16.001%'
         OR faixa_salarial LIKE '%20.001%' OR faixa_salarial LIKE '%24.001%'
         OR faixa_salarial LIKE '%30.001%' OR faixa_salarial LIKE '%40.001%'
         OR faixa_salarial LIKE 'Acima%'
       THEN 1 ELSE 0 END AS top_tier
FROM unificados;

-- Na edição 2025 da pesquisa, 248 respondentes tiveram a atuação registrada como código binário (0/1) ou nulo. 
-- A camada Gold aplica regra de harmonização que os reclassifica conforme o padrão das demais edições, garantindo comparabilidade 2023–2025
-- Como foi transformado todas as colunas em STRING, para evitar erros, foi usado TRY_CAST para evitar que a consulta quebre.


-- ============================= P1: Como está estrururado o mercado brasileiro de Dados =============================

-- 1.1 - Total por ano
SELECT 
    ano, COUNT(*) AS total 
FROM vw 
GROUP BY ano 
ORDER BY ano;

-- 1.2 - Cargos por ano (%)
SELECT ano, atuacao, COUNT(*) AS total,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY ano), 1) AS pct
FROM vw
GROUP BY ano, atuacao
ORDER BY ano, total DESC;

-- 1.3 - Senioridade por ano 
SELECT 
    ano, nivel, COUNT(*) AS total
FROM vw 
GROUP BY ano, nivel 
ORDER BY ano, total DESC;

-- 1.4 - Top 10 setores
SELECT 
    setor, COUNT(*) AS total
FROM vw 
GROUP BY setor 
ORDER BY total DESC 
LIMIT 10;


-- ============================= P2: Quais perfis proficionais são mais valorizados pelo mercado? =============================

-- 2.1 - % de Top Tier por atuação
SELECT atuacao, COUNT(*) AS total,
       ROUND(100.0 * SUM(top_tier) / COUNT(*), 1) AS pct_top
FROM vw
GROUP BY atuacao
ORDER BY pct_top DESC;


-- 2.2 - % de Top Tier por senioridade
SELECT nivel, COUNT(*) AS total,
       ROUND(100.0 * SUM(top_tier) / COUNT(*), 1) AS pct_top
FROM vw 
GROUP BY nivel 
ORDER BY pct_top DESC;

-- 2.3 - % Top Tier por nível de ensino
SELECT nivel_de_ensino,
       COUNT(*) AS total,
       ROUND(100.0 * SUM(top_tier) / COUNT(*), 1) AS pct_top_tier,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct_mercado
FROM vw
WHERE nivel_de_ensino != 'Nao_informado'
GROUP BY nivel_de_ensino
ORDER BY pct_top_tier DESC;


-- ============================= P3: Quail é o cenário de diversidade de gênero nas carreiras de dados? =============================

-- 3.1 - Participação por gênero (%)
SELECT ano,
  ROUND(100.0 * SUM(CASE WHEN genero = 'Feminino' THEN 1 ELSE 0 END) / COUNT(*), 1) AS mulheres,
  ROUND(100.0 * SUM(CASE WHEN genero = 'Masculino' THEN 1 ELSE 0 END) / COUNT(*), 1) AS homens
FROM vw 
GROUP BY ano 
ORDER BY ano;

-- 3.2 - Gap: % no Top Tier por gênero
SELECT ano,
  ROUND(100.0 * SUM(CASE WHEN genero='Feminino' AND top_tier=1 THEN 1 ELSE 0 END) /
        NULLIF(SUM(CASE WHEN genero='Feminino' THEN 1 ELSE 0 END), 0), 1) AS mulheres_top,
  ROUND(100.0 * SUM(CASE WHEN genero='Masculino' AND top_tier=1 THEN 1 ELSE 0 END) /
        NULLIF(SUM(CASE WHEN genero='Masculino' THEN 1 ELSE 0 END), 0), 1) AS homens_top
FROM vw 
GROUP BY ano 
ORDER BY ano;

-- 3.3 - Vivência de preconceito (gênero e raça)

SELECT genero,
       SUM(TRY_CAST(experiencia_prejudicada_devido_a_minha_identidade_de_genero AS INT)) AS preconceito_genero
FROM vw
GROUP BY genero
ORDER BY preconceito_genero DESC;

SELECT cor_raca_etnia, COUNT(*) AS total,
       SUM(TRY_CAST(experiencia_prejudicada_devido_a_minha_cor_raca_etnia AS INT)) AS preconceito_raca
FROM vw
GROUP BY cor_raca_etnia
ORDER BY total DESC;


-- ============================= P4: Quais tecnologias apresentam maior adoção entre os profissionais? =============================

-- 4.1 - Linguagens por ano
SELECT ano,
       SUM(TRY_CAST(python AS INT))     AS python,
       SUM(TRY_CAST("sql" AS INT))      AS sql,
       SUM(TRY_CAST(r AS INT))          AS r,
       SUM(TRY_CAST(julia AS INT))       AS julia,
       SUM(TRY_CAST(rust AS INT)) AS rust,
       SUM(TRY_CAST(scala AS INT))      AS scala,
       SUM(TRY_CAST(nao_utilizo_nenhuma_linguagem AS INT)) AS nenhuma
FROM vw
GROUP BY ano
ORDER BY ano;

-- 4.2 - Bancos de dados por ano
SELECT ano,
       SUM(TRY_CAST(mysql AS INT))          AS mysql,
       SUM(TRY_CAST(postgresql AS INT))     AS postgresql,
       SUM(TRY_CAST(sql_server AS INT))     AS sql_server,
       SUM(TRY_CAST(oracle AS INT))         AS oracle,
       SUM(TRY_CAST(s3 AS INT))             AS s3,
       SUM(TRY_CAST(amazon_redshift AS INT)) AS redshift,
       SUM(TRY_CAST(google_bigquery AS INT)) AS bigquery,
       SUM(TRY_CAST(snowflake AS INT))      AS snowflake,
       SUM(TRY_CAST(databricks AS INT))     AS databricks,
       SUM(TRY_CAST(mongodb AS INT))        AS mongodb
FROM vw
GROUP BY ano
ORDER BY ano;

-- 4.3 - Cloud por ano + cloud preferida
SELECT ano,
       SUM(TRY_CAST(amazon_web_services_aws AS INT)) AS aws,
       SUM(TRY_CAST(azure_microsoft AS INT))         AS azure,
       SUM(TRY_CAST(google_cloud_gcp AS INT))        AS gcp,
       SUM(TRY_CAST(oracle_cloud AS INT))            AS oracle_cloud,
       SUM(TRY_CAST(ibm AS INT))                     AS ibm,
       SUM(TRY_CAST(servidores_on_premise_nao_utilizamos_cloud AS INT)) AS on_premise
FROM vw
GROUP BY ano
ORDER BY ano;

-- 4.3.1 - Cloud PREFERIDA (coluna de texto → COUNT)
SELECT ano, cloud_preferida, COUNT(*) AS total
FROM vw
GROUP BY ano, cloud_preferida
ORDER BY ano, total DESC;

-- 4.4 - Ferramentas de BI por ano + a do dia a dia
SELECT ano,
       SUM(TRY_CAST(microsoft_powerbi AS INT)) AS powerbi,
       SUM(TRY_CAST(tableau AS INT))           AS tableau,
       SUM(TRY_CAST(looker AS INT)) AS looker,
       SUM(TRY_CAST(qlik_view_qlik_sense AS INT)) AS qlik,
       SUM(TRY_CAST(metabase AS INT))          AS metabase,
       SUM(TRY_CAST(grafana AS INT))           AS grafana,
       SUM(TRY_CAST(fazemos_todas_as_analises_utilizando_apenas_excel_ou_planilhas_do_google AS INT)) AS so_excel,
       SUM(TRY_CAST(nao_utilizo_nenhuma_ferramenta_de_bi_no_trabalho AS INT)) AS nenhuma_bi
FROM vw
GROUP BY ano
ORDER BY ano;

-- 4.4.1 - BI do DIA A DIA (texto → COUNT)
SELECT 
    ano, ferramenta_de_bi_utilizada_no_dia_a_dia, COUNT(*) AS total
FROM vw
GROUP BY ano, ferramenta_de_bi_utilizada_no_dia_a_dia;


-- ============================= P5: Qual é o índice de adoção de Inteligência Artificial e seu impacto? =============================

-- 5.1 - Quem usa IA no trabalho (agrupado):
WITH base AS (
  SELECT ano,
         CASE WHEN utiliza_chatgpt_ou_llms_no_trabalho LIKE 'Utilizo%' THEN 'Usa IA no trabalho'
              WHEN utiliza_chatgpt_ou_llms_no_trabalho LIKE 'Não%'    THEN 'Não usa IA'
              ELSE 'Sem informação' END AS uso_ia_grupo
  FROM vw
)
SELECT ano, uso_ia_grupo, COUNT(*) AS total,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY ano), 1) AS pct
FROM base
GROUP BY ano, uso_ia_grupo
ORDER BY ano, total DESC;

-- 5.2 - Quem paga a conta da IA:
SELECT ano,
  ROUND(100.0*SUM(TRY_CAST(nao_uso_solucoes_de_ai_generativa_com_foco_em_produtividade AS INT))/COUNT(*),1) AS nao_usa,
  ROUND(100.0*SUM(TRY_CAST(uso_solucoes_gratuitas_de_ai_generativa_com_foco_em_produtividade AS INT))/COUNT(*),1) AS gratis,
  ROUND(100.0*SUM(TRY_CAST(uso_e_pago_pelas_solucoes_de_ai_generativa_com_foco_em_produtividade AS INT))/COUNT(*),1) AS proprio_bolso,
  ROUND(100.0*SUM(TRY_CAST(a_empresa_que_trabalho_paga_pelas_solucoes_de_ai_generativa_com_foco_em_produtividade AS INT))/COUNT(*),1) AS empresa_paga
FROM vw 
GROUP BY ano 
ORDER BY ano;

-- 5.3 - Como a IA aparece nas empresas:
SELECT ano,
  ROUND(100.0*SUM(TRY_CAST(colaboradores_usando_ai_generativa_de_forma_independente_e_descentralizada AS INT))/COUNT(*),1) AS uso_independente,
  ROUND(100.0*SUM(TRY_CAST(direcionamento_centralizado_do_uso_de_ai_generativa AS INT))/COUNT(*),1) AS centralizado,
  ROUND(100.0*SUM(TRY_CAST(desenvolvedores_utilizando_copilots AS INT))/COUNT(*),1) AS copilots,
  ROUND(100.0*SUM(TRY_CAST(ia_generativa_e_llms_nao_e_prioridade AS INT))/COUNT(*),1) AS nao_prioridade
FROM vw 
GROUP BY ano 
ORDER BY ano;

-- 5.4 - IA é prioridade?
SELECT ano,
  ROUND(100.0*SUM(TRY_CAST(ai_generativa_e_uma_prioridade_em_sua_empresa AS INT))/COUNT(*),1) AS ia_e_prioridade,
  ROUND(100.0*SUM(TRY_CAST(ia_generativa_e_llms_nao_e_prioridade AS INT))/COUNT(*),1) AS nao_e_prioridade
FROM vw 
GROUP BY ano 
ORDER BY ano;

-- 5.5 - Barreiras de adoção por ano:
SELECT ano,
  SUM(TRY_CAST(falta_de_expertise_ou_falta_de_recursos AS INT))                          AS falta_expertise,
  SUM(TRY_CAST(dados_da_empresa_nao_estao_prontos_para_uso_de_ia_generativa AS INT))    AS dados_nao_prontos,
  SUM(TRY_CAST(preocupacoes_com_seguranca_e_privacidade_de_dados AS INT))               AS seguranca_privacidade,
  SUM(TRY_CAST(retorno_sobre_investimento_roi_nao_comprovado_de_ia_generativa AS INT))  AS roi_nao_comprovado,
  SUM(TRY_CAST(falta_de_confiabilidade_das_saidas_alucinacao_dos_modelos AS INT))       AS alucinacao,
  SUM(TRY_CAST(alta_direcao_da_empresa_nao_ve_valor_ou_nao_ve_como_prioridade AS INT))  AS direcao_nao_ve_valor
FROM vw 
GROUP BY ano 
ORDER BY ano;

-- Crescimento 2023 → 2025
SELECT
  SUM(CASE WHEN ano = '2023' THEN 1 ELSE 0 END) AS total_2023,
  SUM(CASE WHEN ano = '2025' THEN 1 ELSE 0 END) AS total_2025,
  ROUND(100.0 * (SUM(CASE WHEN ano = '2025' THEN 1 ELSE 0 END) -
                 SUM(CASE WHEN ano = '2023' THEN 1 ELSE 0 END)) /
                 SUM(CASE WHEN ano = '2023' THEN 1 ELSE 0 END), 1) AS variacao_pct
FROM vw;


-- ============================= P6: EXistem diferenças relevantes entre regiões, senioridades ou modelos de trabnalho? =============================

-- 6.1 - O Mito do Remoto: qual modelo paga mais? (por ano)
SELECT ano,
       atualmente_qual_a_sua_forma_de_trabalho AS modelo,
       COUNT(*) AS total,
       ROUND(100.0 * SUM(TRY_CAST(top_tier AS INT)) / COUNT(*), 1) AS pct_top
FROM vw
GROUP BY ano, atualmente_qual_a_sua_forma_de_trabalho
ORDER BY ano, pct_top DESC;

-- 6.2 - Top Tier por região (por ano)
SELECT ano,
       regiao_onde_mora,
       COUNT(*) AS total,
       ROUND(100.0 * SUM(TRY_CAST(top_tier AS INT)) / COUNT(*), 1) AS pct_top
FROM vw
GROUP BY ano, regiao_onde_mora
ORDER BY ano, pct_top DESC;

-- 6.3 - Realidade (modelo ATUAL por ano)
SELECT ano,
       atualmente_qual_a_sua_forma_de_trabalho AS atual,
       COUNT(*) AS total
FROM vw
GROUP BY ano, atualmente_qual_a_sua_forma_de_trabalho
ORDER BY ano, total DESC;

-- 6.4 - Desejo (modelo IDEAL por ano)
SELECT ano,
       qual_a_forma_de_trabalho_ideal_para_voce AS ideal,
       COUNT(*) AS total
FROM vw
GROUP BY ano, qual_a_forma_de_trabalho_ideal_para_voce
ORDER BY ano, total DESC;


-- ============================= P7: Quais oportunidades e desafios podem ser identificados para empresas que desejam investir em Dados e Inteligência Artificial? =============================

-- 7.1 - Desafios dos Gestores (por ano)
SELECT 
    ano, 'Contratar talentos' AS desafio, SUM(COALESCE(TRY_CAST(a_contratar_novos_talentos AS INT), 0)) AS total 
FROM vw 
GROUP BY ano
UNION ALL

SELECT
    ano, 'Reter talentos', SUM(COALESCE(TRY_CAST(b_reter_talentos AS INT), 0)) 
FROM vw 
GROUP BY ano
UNION ALL

SELECT 
    ano, 'Convencer a investir', SUM(COALESCE(TRY_CAST(c_convencer_a_empresa_a_aumentar_os_investimentos_na_area_de_dados AS INT), 0)) 
FROM vw 
GROUP BY ano
UNION ALL

SELECT 
    ano, 'Gerar valor p/ negócio', SUM(COALESCE(TRY_CAST(h_conseguir_gerar_valor_para_as_areas_de_negocios_atraves_de_estudos_e_experimentos AS INT), 0))   
FROM vw 
GROUP BY ano
UNION ALL

SELECT 
    ano, 'Provar ROI', SUM(COALESCE(TRY_CAST(garantir_retorno_do_investimento_roi_em_projetos_de_dados AS INT), 0)) 
FROM vw 
GROUP BY ano
ORDER BY 
    ano, total DESC;

-- 7.2 - O que os profissionais mais valorizam (por ano)
SELECT 
    ano, 'Remuneração' AS valor, SUM(COALESCE(TRY_CAST(remuneracao_salario AS INT), 0)) AS total 
FROM vw 
GROUP BY ano
UNION ALL

SELECT 
    ano, 'Flexibilidade remota', SUM(COALESCE(TRY_CAST(flexibilidade_de_trabalho_remoto AS INT), 0)) 
FROM vw 
GROUP BY ano

UNION ALL
SELECT 
    ano, 'Plano de carreira', SUM(COALESCE(TRY_CAST(plano_de_carreira_e_oportunidades_de_crescimento_profissional AS INT), 0)) 
FROM vw 
GROUP BY ano

UNION ALL
SELECT 
    ano, 'Aprendizado', SUM(COALESCE(TRY_CAST(oportunidade_de_aprendizado_e_trabalhar_com_referencias_na_area AS INT), 0)) 
FROM vw 
GROUP BY ano

SELECT 
    ano, 'Maturidade em dados', SUM(COALESCE(TRY_CAST(maturidade_da_empresa_em_termos_de_tecnologia_e_dados AS INT), 0)) 
FROM vw GROUP BY ano

UNION ALL
SELECT 
    ano, 'Propósito', SUM(COALESCE(TRY_CAST(proposito_do_trabalho_e_da_empresa AS INT), 0)) 
FROM vw 
GROUP BY ano
ORDER BY ano, total DESC;

-- 7.3 - Maturidade das empresas (Data Lake e DW por ano)
SELECT ano,
  ROUND(100.0 * SUM(CASE WHEN lower(sua_organizacao_possui_um_data_lake)
         IN ('1', '1.0', 'true', 'sim') THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_lake,
  ROUND(100.0 * SUM(CASE WHEN lower(sua_organizacao_possui_um_data_warehouse)
         IN ('1', '1.0', 'true', 'sim') THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_dw
FROM vw
GROUP BY ano
ORDER BY ano;