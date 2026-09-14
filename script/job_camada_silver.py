import sys, re, unicodedata
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
job = Job(glueContext); job.init(args['JOB_NAME'], args)


BUCKET = 'SEU-BUCKET'

# ================= LIMPEZA DE NOMES (Spark) =================
def normalizar(n):
    n = str(n).strip().lower()
    n = ''.join(c for c in unicodedata.normalize('NFD', n)
                if unicodedata.category(c) != 'Mn')
    n = re.sub(r'[^a-z0-9]+', '_', n)
    return n.strip('_')

def limpar_nome(col):
    s = str(col).strip()
    if s.startswith("('") or s.startswith('("'):          # tupla do 2023: "('P0', 'id')"
        sep = "', '" if "', '" in s else '", '
        if sep in s:        s = s.split(sep, 1)[1].rstrip("')")
        elif '", ' in s:    s = s.split('", ', 1)[1].rstrip('")')
    s = re.sub(r'^\d+(\.[a-zA-Z0-9]+)*[\s_]+', '', s)    # código 0.a_ / 1.a.1_
    return normalizar(s)

def dedup_cols(names):                                     # gêmeas -> sufixo _1, _2
    seen, out = {}, []
    for n in names:
        if n in seen:
            seen[n] += 1; out.append(f"{n}_{seen[n]}")
        else:
            seen[n] = 0; out.append(n)
    return out

def carregar(path):
    df = spark.read.option('header', True).option('inferSchema', True).csv(path)
    return df.toDF(*dedup_cols([limpar_nome(c) for c in df.columns]))   # toDF nao parseia nome!

def renomear(df, d):                                     # dicionario ja sanitizado na hora
    for k, v in d.items():
        df = df.withColumnRenamed(normalizar(k), normalizar(v))
    return df

# ================= DICIONARIOS (copiados do meu notebook do colab) =================
traducao = {
 'id':'token','experiencia_prejudicada_devido_a_minha_identidade_de_genero':'sim,_devido_a_minha_identidade_de_genero',
 'experiencia_prejudicada_devido_a_minha_cor_raca_etnia':'sim,_devido_a_minha_cor_raca_etnia',
 'experiencia_prejudicada_devido_ao_fato_de_ser_pcd':'sim,_devido_ao_fato_de_ser_pcd',
 'qual_sua_situacao_atual_de_trabalho':'situacao_de_trabalho',
 'quanto_tempo_de_experiencia_na_area_de_dados_voce_tem':'tempo_de_experiencia_em_dados',
 'quanto_tempo_de_experiencia_na_area_de_ti_engenharia_de_software_voce_teve_antes_de_comecar_a_trabalhar_na_area_de_dados':'tempo_de_experiencia_em_ti',
 'voce_esta_satisfeito_na_sua_empresa_atual':'satisfeito_atualmente',
 'qual_o_principal_motivo_da_sua_insatisfacao_com_a_empresa_atual':'motivo_insatisfacao',
 'atualmente_qual_a_sua_forma_de_trabalho':'forma_de_trabalho_atual',
 'qual_a_forma_de_trabalho_ideal_para_voce':'forma_de_trabalho_ideal',
 'atuacao':'atuacao_no_dia_a_dia','nao_utilizo_nenhuma_linguagem':'nenhuma_linguagem',
 'entre_as_linguagens_listadas_abaixo,_qual_e_a_que_voce_mais_utiliza_no_trabalho':'linguagem_mais_utilizada',
 'ferramenta_de_bi_utilizada_no_dia_a_dia':'ferramenta_de_bi_do_dia_a_dia',
 'utiliza_chatgpt_ou_llms_no_trabalho':'usa_chatgpt_ou_llms_no_trabalho',
 'ai_generativa_e_uma_prioridade_em_sua_empresa':'ia_generativa_e_uma_prioridade_em_sua_empresa',
 'ai_generativa_e_llms_para_melhorar_produtos_externos':'ia_generativa_e_llms_para_melhorar_produtos_externos',
 'a_contratar_novos_talentos':'contratar_novos_talentos','b_reter_talentos':'reter_talentos',
 'c_convencer_a_empresa_a_aumentar_os_investimentos_na_area_de_dados':'convencer_a_empresa_a_aumentar_os_investimentos_na_area_de_dados'}

traducao_final = {
 'modelo_de_trabalho_atual':'atualmente_qual_a_sua_forma_de_trabalho',
 'modelo_de_trabalho_ideal':'qual_a_forma_de_trabalho_ideal_para_voce',
 'atitude_em_caso_de_retorno_presencial':'caso_sua_empresa_decida_pelo_modelo_100pct_presencial_qual_sera_sua_atitude',
 'gostaria_de_trabalhar_em_outra_area':'gostaria_de_trabalhar_em_em_outra_area_de_atuacao',
 'oportunidade_de_aprendizado_e_trabalhar_com_referencias':'oportunidade_de_aprendizado_e_trabalhar_com_referencias',
 'plano_de_carreira_e_oportunidades_de_crescimento':'plano_de_carreira_e_oportunidades_de_crescimento',
 'contratar_talentos':'a_contratar_novos_talentos',
 'convencer_a_empresa_a_aumentar_investimentos':'c_convencer_a_empresa_a_aumentar_os_investimentos_na_area_de_dados',
 'gestao_de_equipes_no_ambiente_remoto':'d_gestao_de_equipes_no_ambiente_remoto',
 'gestao_de_projetos_envolvendo_areas_multidisciplinares':'e_gestao_de_projetos_envolvendo_areas_multidisciplinares',
 'organizar_as_informacoes_com_qualidade_e_confiabilidade':'f_organizar_as_informacoes_com_qualidade_e_confiabilidade',
 'processar_e_armazenar_um_alto_volume_de_dados':'g_processar_e_armazenar_um_alto_volume_de_dados',
 'gerar_valor_para_as_areas_de_negocios':'h_gerar_valor_para_as_areas_de_negocios',
 'desenvolver_e_manter_modelos_machine_learning_em_producao':'i_desenvolver_e_manter_modelos_machine_learning_em_producao',
 'gerenciar_a_expectativa_das_areas':'j_gerenciar_a_expectativa_das_areas',
 'garantir_a_manutencao_dos_projetos_e_modelos_em_producao':'k_garantir_a_manutencao_dos_projetos_e_modelos_em_producao',
 'conseguir_levar_inovacao_para_a_empresa':'conseguir_levar_inovacao_para_a_empresa',
 'garantir_roi_em_projetos_de_dados':'garantir_roi_em_projetos_de_dados',
 'ai_generativa_e_uma_prioridade_em_sua_empresa':'ai_generativa_e_llm_e_uma_prioridade',
 'ai_generativa_e_llms_para_melhorar_produtos_externos':'ai_generativa_e_llms_para_melhorar_produtos_externos_para_os_clientes_finais',
 'atuacao':'atuacao_em_dados','nao_utilizo_nenhuma_linguagem':'nao_utilizo_nenhuma_das_linguagens_listadas',
 'entre_as_linguagens_listadas_abaixo,_qual_e_a_que_voce_mais_utiliza_no_trabalho':'linguagem_mais_usada',
 'ferramenta_de_bi_utilizada_no_dia_a_dia':'ferramenta_de_bi_dia_a_dia',
 'utiliza_chatgpt_ou_llms_no_trabalho':'usa_chatgpt_ou_copilot_no_trabalho',
 'sua_organizacao_possui_um_data_lake':'possui_data_lake',
 'sua_organizacao_possui_um_data_warehouse':'possui_data_warehouse'}
 
traducao_restante = {
    'modelo_de_trabalho_atual': 'atualmente_qual_a_sua_forma_de_trabalho',
    'modelo_de_trabalho_ideal': 'qual_a_forma_de_trabalho_ideal_para_voce',
    'atitude_em_caso_de_retorno_presencial': 'caso_sua_empresa_decida_pelo_modelo_100pct_presencial_qual_sera_sua_atitude',
    'contratar_talentos': 'a_contratar_novos_talentos',
    'convencer_a_empresa_a_aumentar_investimentos': 'c_convencer_a_empresa_a_aumentar_os_investimentos_na_area_de_dados',
    'gestao_de_equipes_no_ambiente_remoto': 'd_gestao_de_equipes_no_ambiente_remoto',
    'gestao_de_projetos_envolvendo_areas_multidisciplinares': 'e_gestao_de_projetos_envolvendo_areas_multidisciplinares_da_empresa',
    'organizar_as_informacoes_com_qualidade_e_confiabilidade': 'f_organizar_as_informacoes_e_garantir_a_qualidade_e_confiabilidade',
    'processar_e_armazenar_um_alto_volume_de_dados': 'g_conseguir_processar_e_armazenar_um_alto_volume_de_dados',
    'gerar_valor_para_as_areas_de_negocios': 'h_conseguir_gerar_valor_para_as_areas_de_negocios_atraves_de_estudos_e_experimentos',
    'desenvolver_e_manter_modelos_machine_learning_em_producao': 'i_desenvolver_e_manter_modelos_machine_learning_em_producao',
    'gerenciar_a_expectativa_das_areas': 'j_gerenciar_a_expectativa_das_areas_de_negocio_em_relacao_as_entregas_das_equipes_de_dados',
    'garantir_a_manutencao_dos_projetos_e_modelos_em_producao': 'k_garantir_a_manutencao_dos_projetos_e_modelos_em_producao,_em_meio_ao_crescimento_da_empresa',
    'conseguir_levar_inovacao_para_a_empresa': 'conseguir_levar_inovacao_para_a_empresa_atraves_dos_dados',
    'garantir_roi_em_projetos_de_dados': 'garantir_retorno_do_investimento_roi_em_projetos_de_dados',
    'ai_generativa_e_llm_e_uma_prioridade': 'ai_generativa_e_uma_prioridade_em_sua_empresa',
    'ai_generativa_e_llms_para_melhorar_produtos_externos_para_os_clientes_finais': 'ai_generativa_e_llms_para_melhorar_produtos_externos',
    'atuacao_em_dados': 'atuacao',
    'nao_utilizo_nenhuma_das_linguagens_listadas': 'nao_utilizo_nenhuma_linguagem',
    'linguagem_mais_usada': 'entre_as_linguagens_listadas_abaixo,_qual_e_a_que_voce_mais_utiliza_no_trabalho',
    'ferramenta_de_bi_dia_a_dia': 'ferramenta_de_bi_utilizada_no_dia_a_dia',
    'usa_chatgpt_ou_copilot_no_trabalho': 'utiliza_chatgpt_ou_llms_no_trabalho',
    'possui_data_lake': 'sua_organizacao_possui_um_data_lake',
    'possui_data_warehouse': 'sua_organizacao_possui_um_data_warehouse',
    'plano_de_carreira_e_oportunidades_de_crescimento': 'plano_de_carreira_e_oportunidades_de_crescimento_profissional',
}

correcao_troca = {
 'remuneracao_salario':'salario_atual_nao_corresponde_ao_mercado',
 'beneficios':'gostaria_de_receber_mais_beneficios',
 'ambiente_e_clima_de_trabalho':'o_clima_de_trabalho_ambiente_nao_e_bom',
 'maturidade_da_empresa_em_termos_de_tecnologia_e_dados':'falta_de_maturidade_analitica_na_empresa',
 'oportunidades_de_crescimento':'falta_de_oportunidade_de_crescimento_no_emprego_atual',
 'relacao_com_os_gestores_e_lideres':'nao_tenho_uma_boa_relacao_com_meu_lider_gestor',
 'gostaria_de_trabalhar_em_outra_area':'gostaria_de_trabalhar_em_em_outra_area_de_atuacao',
 'remuneracao_salario__1':'remuneracao_salario','beneficios__1':'beneficios',
 'proposito_do_trabalho_e_da_empresa__1':'proposito_do_trabalho_e_da_empresa',
 'flexibilidade_de_trabalho_remoto__1':'flexibilidade_de_trabalho_remoto',
 'ambiente_e_clima_de_trabalho__1':'ambiente_e_clima_de_trabalho',
 'oportunidade_de_aprendizado_e_trabalhar_com_referencias__1':'oportunidade_de_aprendizado_e_trabalhar_com_referencias_na_area',
 'maturidade_da_empresa_em_termos_de_tecnologia_e_dados__1':'maturidade_da_empresa_em_termos_de_tecnologia_e_dados',
 'reputacao_que_a_empresa_tem_no_mercado__1':'reputacao_que_a_empresa_tem_no_mercado',
 'relacao_com_os_gestores_e_lideres__1':'qualidade_dos_gestores_e_lideres'}

traducao_esquecida = {
 'token':'id',
 'sim,_devido_a_minha_identidade_de_genero':'experiencia_prejudicada_devido_a_minha_identidade_de_genero',
 'sim,_devido_a_minha_cor_raca_etnia':'experiencia_prejudicada_devido_a_minha_cor_raca_etnia',
 'sim,_devido_ao_fato_de_ser_pcd':'experiencia_prejudicada_devido_ao_fato_de_ser_pcd'}

traducao_extra_2025 = {
 'situacao_de_trabalho':'qual_sua_situacao_atual_de_trabalho',
 'tempo_de_experiencia_em_dados':'quanto_tempo_de_experiencia_na_area_de_dados_voce_tem',
 'tempo_de_experiencia_em_ti':'quanto_tempo_de_experiencia_na_area_de_ti_engenharia_de_software_voce_teve_antes_de_comecar_a_trabalhar_na_area_de_dados',
 'satisfeito_atualmente':'voce_esta_satisfeito_na_sua_empresa_atual',
 'motivo_insatisfacao':'qual_o_principal_motivo_da_sua_insatisfacao_com_a_empresa_atual',
 'reter_talentos':'b_reter_talentos'}

# ---------- LISTA SILVER (as colunas que a gente quer manter) ----------
colunas_silver = [
 'id', 'idade', 'faixa_idade', 'genero', 'cor_raca_etnia', 'pcd',
 'experiencia_prejudicada_devido_a_minha_identidade_de_genero',
 'experiencia_prejudicada_devido_a_minha_cor_raca_etnia',
 'experiencia_prejudicada_devido_ao_fato_de_ser_pcd',
 'quantidade_de_oportunidades_de_emprego_vagas_recebidas',
 'senioridade_das_vagas_recebidas_em_relacao_a_sua_experiencia',
 'aprovacao_em_processos_seletivos_entrevistas',
 'oportunidades_de_progressao_de_carreira',
 'estado_onde_mora', 'uf_onde_mora', 'regiao_onde_mora',
 'nivel_de_ensino', 'area_de_formacao',
 'qual_sua_situacao_atual_de_trabalho', 'setor', 'cargo_atual', 'nivel', 'faixa_salarial',
 'quanto_tempo_de_experiencia_na_area_de_dados_voce_tem',
 'quanto_tempo_de_experiencia_na_area_de_ti_engenharia_de_software_voce_teve_antes_de_comecar_a_trabalhar_na_area_de_dados',
 'atualmente_qual_a_sua_forma_de_trabalho',
 'qual_a_forma_de_trabalho_ideal_para_voce',
 'caso_sua_empresa_decida_pelo_modelo_100%_presencial_qual_sera_sua_atitude',
 'voce_esta_satisfeito_na_sua_empresa_atual',
 'qual_o_principal_motivo_da_sua_insatisfacao_com_a_empresa_atual',
 'falta_de_oportunidade_de_crescimento_no_emprego_atual',
 'salario_atual_nao_corresponde_ao_mercado',
 'nao_tenho_uma_boa_relacao_com_meu_lider_gestor',
 'gostaria_de_trabalhar_em_em_outra_area_de_atuacao',
 'gostaria_de_receber_mais_beneficios',
 'o_clima_de_trabalho_ambiente_nao_e_bom',
 'falta_de_maturidade_analitica_na_empresa','proposito_do_trabalho_e_da_empresa',
 'remuneracao_salario', 'beneficios',
 'flexibilidade_de_trabalho_remoto', 'ambiente_e_clima_de_trabalho',
 'oportunidade_de_aprendizado_e_trabalhar_com_referencias_na_area',
 'plano_de_carreira_e_oportunidades_de_crescimento_profissional',
 'maturidade_da_empresa_em_termos_de_tecnologia_e_dados',
 'qualidade_dos_gestores_e_lideres', 'reputacao_que_a_empresa_tem_no_mercado',
 'a_contratar_novos_talentos.', 'b_reter_talentos.',
 'c_convencer_a_empresa_a_aumentar_os_investimentos_na_area_de_dados.',
 'd_gestao_de_equipes_no_ambiente_remoto.',
 'e_gestao_de_projetos_envolvendo_areas_multidisciplinares_da_empresa.',
 'f_organizar_as_informacoes_e_garantir_a_qualidade_e_confiabilidade.',
 'g_conseguir_processar_e_armazenar_um_alto_volume_de_dados.',
 'h_conseguir_gerar_valor_para_as_areas_de_negocios_atraves_de_estudos_e_experimentos.',
 'i_desenvolver_e_manter_modelos_machine_learning_em_producao.',
 'j_gerenciar_a_expectativa_das_areas_de_negocio_em_relacao_as_entregas_das_equipes_de_dados.',
 'k_garantir_a_manutencao_dos_projetos_e_modelos_em_producao,_em_meio_ao_crescimento_da_empresa.',
 'conseguir_levar_inovacao_para_a_empresa_atraves_dos_dados.',
 'garantir_retorno_do_investimento_roi_em_projetos_de_dados.',
 'dividir_o_tempo_entre_entregas_tecnicas_e_gestao.',
 'ai_generativa_e_uma_prioridade_em_sua_empresa',
 'colaboradores_usando_ai_generativa_de_forma_independente_e_descentralizada',
 'direcionamento_centralizado_do_uso_de_ai_generativa',
 'desenvolvedores_utilizando_copilots',
 'ai_generativa_e_llms_para_melhorar_produtos_externos',
 'ai_generativa_e_llms_para_melhorar_produtos_internos_para_os_colaboradores',
 'ia_generativa_e_llms_como_principal_frente_do_negocio',
 'ia_generativa_e_llms_nao_e_prioridade',
 'nao_sei_opinar_sobre_o_uso_de_ia_generativa_e_llms_na_empresa',
 'falta_de_compreensao_dos_casos_de_uso',
 'falta_de_confiabilidade_das_saidas_alucinacao_dos_modelos',
 'incerteza_em_relacao_a_regulamentacao',
 'preocupacoes_com_seguranca_e_privacidade_de_dados',
 'retorno_sobre_investimento_roi_nao_comprovado_de_ia_generativa',
 'dados_da_empresa_nao_estao_prontos_para_uso_de_ia_generativa',
 'falta_de_expertise_ou_falta_de_recursos',
 'alta_direcao_da_empresa_nao_ve_valor_ou_nao_ve_como_prioridade',
 'preocupacoes_com_propriedade_intelectual',
 'atuacao',
 'sql', 'r', 'python', 'c_c++_c#', '.net', 'java', 'julia', 'sas_stata',
 'visual_basic_vba', 'scala', 'matlab', 'rust', 'php', 'javascript',
 'nao_utilizo_nenhuma_linguagem',
 'entre_as_linguagens_listadas_abaixo, qual_e_a_que_voce_mais_utiliza_no_trabalho',
 'mysql', 'oracle', 'sql_server', 'amazon_aurora_ou_rds', 'dynamodb', 'coachdb',
 'cassandra', 'mongodb', 'mariadb', 'datomic', 's3', 'postgresql', 'elasticsearch',
 'db2', 'microsoft_access', 'sqlite', 'sybase', 'firebase', 'vertica', 'redis',
 'neo4j', 'google_bigquery', 'google_firestore', 'amazon_redshift', 'amazon_athena',
 'snowflake', 'databricks', 'hbase', 'presto', 'splunk', 'sap_hana', 'hive', 'firebird',
 'azure_microsoft', 'amazon_web_services_aws', 'google_cloud_gcp', 'oracle_cloud',
 'ibm', 'servidores_on_premise_nao_utilizamos_cloud', 'cloud_propria', 'cloud_preferida',
 'ferramenta_de_bi_utilizada_no_dia_a_dia',
 'microsoft_powerbi', 'qlik_view_qlik_sense', 'tableau', 'metabase', 'superset',
 'redash', 'looker', 'looker_studiogoogle_data_studio', 'amazon_quicksight', 'mode',
 'alteryx', 'microstrategy', 'ibm_analytics_cognos', 'sap_business_objects_sap_analytics',
 'oracle_business_intelligence', 'salesforce_einstein_analytics', 'birst',
 'sas_visual_analytics', 'grafana', 'tibco_spotfire', 'pentaho',
 'fazemos_todas_as_analises_utilizando_apenas_excel_ou_planilhas_do_google',
 'nao_utilizo_nenhuma_ferramenta_de_bi_no_trabalho',
 'utiliza_chatgpt_ou_llms_no_trabalho?',
 'nao_uso_solucoes_de_ai_generativa_com_foco_em_produtividade',
 'uso_solucoes_gratuitas_de_ai_generativa_com_foco_em_produtividade',
 'uso_e_pago_pelas_solucoes_de_ai_generativa_com_foco_em_produtividade',
 'a_empresa_que_trabalho_paga_pelas_solucoes_de_ai_generativa_com_foco_em_produtividade',
 'uso_solucoes_do_tipo_copilot',
 'scripts_python', 'sql &_stored_procedures', 'apache_airflow', 'apache_nifi', 'luigi',
 'aws_glue', 'talend', 'pentaho', 'alteryx', 'stitch', 'fivetran', 'google_dataflow',
 'oracle_data_integrator', 'ibm_datastage', 'sap_bw_etl', 'sql_server_integration_services_ssis',
 'sas_data_integration', 'qlik_sense', 'knime', 'databricks', 'nao_utilizo_ferramentas_de_etl',
 'sua_organizacao_possui_um_data_lake?',
 'sua_organizacao_possui_um_data_warehouse?',
 'ambientes_de_desenvolvimento_na_nuvem_google_colab,_aws_sagemaker,_kaggle_notebooks_etc',
 'plataformas_de_machine_learning_tensorflow,_azure_machine_learning,_kubeflow_etc',
 'feature_store_feast,_hopsworks,_aws_feature_store,_databricks_feature_store_etc',
 'sistemas_de_controle_de_versao_github,_dvc,_neptune,_gitlab_etc',
 'plataformas_de_data_apps_streamlit,_shiny,_plotly_dash_etc',
]

# ================= HELPERS =================
def drop_dup_cols(df):
    """Remove colunas com nome duplicado, mantendo a ÚLTIMA"""
    from collections import Counter
    from pyspark.sql.types import StructType
    
    cols = df.columns
    col_counts = Counter(cols)
    
    # Se não tem duplicata, retorna como está
    if all(count == 1 for count in col_counts.values()):
        return df
    
    # Encontra o índice da ÚLTIMA ocorrência de cada coluna
    seen = {}
    for i, col in enumerate(cols):
        seen[col] = i
    
    keep_indices = sorted(seen.values())
    
    # Processa pelo RDD (evita problema de ambiguidade de nomes)
    rdd = df.rdd.map(lambda row: tuple(row[i] for i in keep_indices))
    
    # Cria novo schema só com as colunas mantidas
    new_schema = StructType([df.schema.fields[i] for i in keep_indices])
    
    # Retorna novo DataFrame
    return spark.createDataFrame(rdd, schema=new_schema)

def selecionar(df):
    # 1) Mata colunas duplicadas ANTES de qualquer select
    df = drop_dup_cols(df)

    # 2) Seleciona as colunas silver
    mapa = {normalizar(c): c for c in df.columns}
    cols = []
    for c in colunas_silver:
        real = mapa.get(normalizar(c))
        if real and real not in cols:
            cols.append(real)
    df = df.select(cols)

    # 3) TRAVA 3: limpa %, ? e ponto final
    ren = {}
    for c in df.columns:
        limpo = c.replace('%', 'pct').replace('?', '').rstrip('.')
        if limpo != c:
            ren[c] = limpo
    df = renomear(df, ren)

    # 4) Garantia extra
    return drop_dup_cols(df)

def finalizar(df, ano):                                  # ← MINHA FUNÇÃO ORIGINAL
    df = df.dropDuplicates(['id'])
    num = [f.name for f in df.schema.fields if f.dataType.typeName() in ('double','float','int','bigint','long')]
    binarias = []
    if num:
        exprs = [F.min(c).alias(f'mn_{i}') for i, c in enumerate(num)] + \
                [F.max(c).alias(f'mx_{i}') for i, c in enumerate(num)]
        row = df.agg(*exprs).first()
        for i, c in enumerate(num):
            mn, mx = row[f'mn_{i}'], row[f'mx_{i}']
            if mn is None or (mn >= 0 and mx <= 1):
                binarias.append(c)
    if binarias:
        df = df.fillna({c: 0 for c in binarias})
        for c in binarias:
            df = df.withColumn(c, F.col(c).cast('int'))
    txt = [f.name for f in df.schema.fields if f.dataType.typeName() == 'string' and f.name != 'id']
    df = df.fillna({c: 'Nao_informado' for c in txt})
    return df.withColumn('ano', F.lit(ano))

# ================= PIPELINE =================
df23 = carregar(f's3://{BUCKET}/bronze/ano_2023/state_of_data_2023.csv')
df24 = carregar(f's3://{BUCKET}/bronze/ano_2024/state_of_data_2024.csv')
df25 = carregar(f's3://{BUCKET}/bronze/ano_2025/state_of_data_2025.csv')

def renomear_e_limpar(df, d):
    """Renomeia e remove duplicatas geradas"""
    df = renomear(df, d)
    return drop_dup_cols(df)

# 2024: mesma ordem do Colab (MATA DUPLICATAS APÓS CADA RENOMEAR)
df24 = renomear(df24, {v: k for k, v in traducao.items()})
df24 = drop_dup_cols(df24)
df24 = renomear(df24, {v: k for k, v in traducao_final.items()})
df24 = drop_dup_cols(df24)
df24 = renomear(df24, traducao_restante)
df24 = drop_dup_cols(df24)
df24 = renomear(df24, correcao_troca)
df24 = drop_dup_cols(df24)

# 2025: mesma ordem do Colab
df25 = renomear(df25, traducao_restante)
df25 = drop_dup_cols(df25)
df25 = renomear(df25, correcao_troca)
df25 = drop_dup_cols(df25)
df25 = renomear(df25, traducao_esquecida)
df25 = drop_dup_cols(df25)
df25 = renomear(df25, traducao_extra_2025)
df25 = drop_dup_cols(df25)

df23 = finalizar(selecionar(df23), 2023)
df24 = finalizar(drop_dup_cols(selecionar(df24)), 2024)
df25 = finalizar(drop_dup_cols(selecionar(df25)), 2025)

df23.write.mode('overwrite').parquet(f's3://{BUCKET}/silver/state_2023/')
df24.write.mode('overwrite').parquet(f's3://{BUCKET}/silver/state_2024/')
df25.write.mode('overwrite').parquet(f's3://{BUCKET}/silver/state_2025/')

comuns = [c for c in df23.columns if c in set(df24.columns) and c in set(df25.columns)]
ordem = ['id', 'ano'] + [c for c in comuns if c not in ('id', 'ano')]

def unificar_schemas(df23, df24, df25):
    """Converte todos pra STRING pra evitar problemas de tipo"""
    from pyspark.sql import functions as F
    
    for df_name, df in [("df23", df23), ("df24", df24), ("df25", df25)]:
        print(f"\nSchema de {df_name}:")
        df.printSchema()
    
    # Identifica todas as colunas de todos os dfs
    all_cols = set(df23.columns) | set(df24.columns) | set(df25.columns)
    
    # Converte tudo pra STRING (mais seguro para unir os df, pois estava dando erro)
    for col in all_cols:
        if col in df23.columns:
            df23 = df23.withColumn(col, F.col(col).cast("string"))
        if col in df24.columns:
            df24 = df24.withColumn(col, F.col(col).cast("string"))
        if col in df25.columns:
            df25 = df25.withColumn(col, F.col(col).cast("string"))
    
    return df23, df24, df25

# ANTES do unionByName
df23, df24, df25 = unificar_schemas(df23, df24, df25)

comuns = [c for c in df23.columns if c in set(df24.columns) and c in set(df25.columns)]
ordem = ['id', 'ano'] + [c for c in comuns if c not in ('id', 'ano')]
df_uni = df23.select(*ordem).unionByName(df24.select(*ordem)).unionByName(df25.select(*ordem))

df_uni = df23.select(*ordem).unionByName(df24.select(*ordem)).unionByName(df25.select(*ordem))
df_uni.write.mode('overwrite').parquet(f's3://{BUCKET}/silver/unificados/')

# PROVA REAL (tem que bater com o Colab!)
print('2023:', df23.count(), len(df23.columns))   # esperado ~5293 / 193
print('2024:', df24.count(), len(df24.columns))   # esperado ~5215 / 188
print('2025:', df25.count(), len(df25.columns))   # esperado ~3494 / 176
print('UNI :', df_uni.count(), len(df_uni.columns))  # esperado ~14002 / 176

job.commit()
spark.stop()

