# Tech Challenge — State of Data Brazil (2023–2025)

**Pipeline de dados em nuvem (arquitetura medalhão) + análises em SQL + Power BI**

Projeto end-to-end que ingere, harmoniza e analisa três edições da pesquisa
**State of Data Brazil** (2023,2024 e 2025), respondendo às perguntas de negócio do challenge com
dados reais de **+14 mil profissionais** da área de dados no país.

---

## Arquitetura

```
┌─────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   BRONZE    │    │     SILVER       │    │      GOLD       │
│  S3 (CSV)   │───▶│  AWS Glue        │───▶│  Amazon Athena  │───▶ Power BI
│  3 edições  │    │  (PySpark)       │    │  (SQL + view)   │
└─────────────┘    └──────────────────┘    └─────────────────┘
      ▲                    │                        │
      │                    ▼                        ▼
 upload manual      limpeza, tipagem       análise exploratória
 (dados brutos)      + unificado           (criação de tabelas)
```

| Camada | Tecnologia | Formato | Descrição |
|---|---|---|---|
| Bronze | Amazon S3 | CSV | Dados crus das pesquisas 2023, 2024 e 2025 |
| Silver | AWS Glue (PySpark) | Parquet | Limpeza, tradução entre anos, dedup e unificação |
| Gold | Amazon Athena (SQL) | Tabelas/Views | Indicadores e respostas às perguntas P1–P7 |
| Apresentação | Power BI | — | Dashboard com os arquivos exportados da silver/gold |


## Camada Bronze

- **Fonte:** pesquisa pública anual *State of Data Brazil* (2023, 2024, 2025).
- **Decisão de ingestão:** como a fonte é um *download público anual*, 
  o carregamento inicial dos CSVs no bucket foi feito via
  upload manual, **simulando a chegada de cada nova edição**. A estrutura de
  pastas é criada/reproduzível pelo script `scripts/setup_s3.py`.
- **Layout:**

```
s3://[seu-bucket]/
├── bronze/ano_2023/state_of_data_2023.csv
├── bronze/ano_2024/state_of_data_2024.csv
├── bronze/ano_2025/state_of_data_2025.csv
├── silver/state_2023/ | state_2024/ | state_2025/ | silver_unificado/
├── gold/
└── athena-resultados/
```

---

## Camada Silver (AWS Glue — PySpark)

O job `job_camada_silver.py` replica em nuvem, passo a passo, a limpeza
construída e validada no notebook Colab:

1. **Limpeza de nomes de colunas**
   - 2023: cabeçalhos em formato de tupla `('P2_o_1 ', 'Remuneração/Salário')`;
   - 2024/2025: prefixos de código (`2.o.1_Remuneração/Salário`);
   - minúsculas, sem acentos, símbolos normalizados (`/ - ? ( ) ' "` → `_`).
2. **Deduplicação de colunas gêmeas** (sufixos `_1`, `_2`).
3. **Dicionários de tradução entre anos** (`traducao`, `traducao_final`,
   `traducao_restante`, `correcao_troca`, `traducao_esquecida`,
   `traducao_extra_2025`), o padrão de nomes do **2023 é a referência**.
4. **Correção da troca insatisfação × critérios** (`correcao_troca`).
5. **Seleção das colunas da silver** (lista curada `colunas_silver`).
6. **Finalização:** dedup de ids, binárias `NaN→0`, texto `NaN→'Nao_informado'`,
   tag `ano`.
7. **Unificação** pela interseção de colunas dos 3 anos → `silver_unificado`.

### Prova real (silver idêntica ao Colab)

| Ano | Linhas | Colunas |
|---|---|---|
| 2023 | 5.293 | 193 |
| 2024 | 5.215 | 188 |
| 2025 | 3.494 | 176 |
| **Unificado** | **14.002** | **176** |

>  Algumas colunas foram removidas pelas edições mais novas da pesquisa
>  ficam naturalmente fora da interseção,
> decisão documentada, não é perda de dados.

---

## Camada Gold (Amazon Athena — SQL)

- Catálogo via **Glue Crawler** sobre a silver.
- **View `vw`** com a tag de negócio `top_tier` (faixas salariais ≥ R$ 12.001/mês).
- **Regra de harmonização de atuação**: respondentes de 2025 cuja
  atuação veio codificada (`0`/`1`/`Nao_informado`) são reclassificados como
  `Gestor`, e o caso isolado de 2023 como `Análise de Dados`, garantindo
  comparabilidade entre anos (mesmo comportamento da limpeza original no Colab).
- Consultas organizadas por pergunta de negócio (`sql/queries_gold.sql`):

---

## Apresentação e Storytelling

Os indicadores calculados na camada Gold alimentaram dashboards interativos no **Power BI**,
permitindo a exploração dinâmica dos dados.

Para a entrega final, os principais insights foram consolidados em um **relatório executivo**
visual, focado na comunicação clara dos resultados para stakeholders e na tomada de decisão
baseada em dados.

---

## Tecnologias

Python · Pandas · PySpark · AWS (S3, Glue, Athena) · SQL · DuckDB ·
Power BI · Git/GitHub

---

> *"Dados crus são opinião de terceiros; dados harmonizados são decisão de engenharia."* 