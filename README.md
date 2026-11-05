# Contexto Brasil — Lakehouse medalhão (Databricks)

Câmbio (BCB PTAX + SGS, BCE), inflação e juros (BCB SGS), feriados (BrasilAPI + Nager.Date),
municípios e população (IBGE) e meteorologia histórica, recente e previsão (Open-Meteo),
em **bronze → silver → gold**, atualizados automaticamente.

## A decisão: notebooks **e** Spark Declarative Pipeline, cada um no seu papel

| Camada | Ferramenta | Porquê |
|---|---|---|
| Ingestão (APIs → landing zone) | **Notebooks Python** | Chamar APIs REST é trabalho com efeitos laterais: paginação, limites de 10 anos, *rate limits*, retries, watermarks. Um pipeline declarativo exige funções sem efeitos laterais que devolvam um DataFrame, por isso isto não cabe lá. |
| Bronze → Silver → Gold | **Lakeflow Spark Declarative Pipeline** | Checkpoints, dependências, retries e ordem de execução são automáticos; *expectations* dão qualidade de dados com métricas; **AUTO CDC** faz upserts e SCD tipo 2 sem escrever `MERGE`; *materialized views* recalculam só o que mudou; linhagem no Unity Catalog. Passar de agendado para contínuo é mudar `continuous: true`. |

Fazer tudo em notebooks também funcionaria, mas teria de escrever à mão os `MERGE`, os checkpoints,
a deduplicação, o SCD2, a ordem entre tabelas e as validações. Fazer tudo no pipeline não funciona bem,
porque as APIs não são fontes de streaming.

### Sobre o "streaming"

Todas as camadas leem em **streaming** (Auto Loader + streaming tables), por isso cada ficheiro novo é
processado uma única vez e de forma incremental. Mas nenhuma destas APIs empurra eventos: publicam no
máximo de hora a hora. Um pipeline em modo contínuo ficaria ligado 24 h à espera de dados que chegam
uma vez por dia, a pagar computação sem ganhar frescura. Por isso o job corre **de hora em hora**
(minuto 7) e cada notebook decide sozinho se há algo novo a pedir:

| Fonte | Frequência da fonte | O que o job faz em cada hora |
|---|---|---|
| PTAX (BCB) | boletins ao longo do dia útil | pede desde o último dia carregado (−10 dias) |
| SGS (câmbio, IPCA, Selic…) | diária / mensal | idem, por série |
| BCE | dias úteis ~16:00 CET | idem |
| Open-Meteo atual + previsão | 15 min / ~horária | a cada 3 h (widget `horas_entre_previsoes`) |
| Open-Meteo arquivo | diária, ~5 dias de atraso | só quando há um dia novo |
| Feriados | anual | ano atual e seguinte, 1×/dia |
| IBGE | anual | 1×/semana |

## Estrutura

```
databricks.yml                       bundle (alvos dev / prod, variáveis)
resources/contexto_brasil.job.yml    job horário: 6 notebooks + pipeline
resources/contexto_brasil.pipeline.yml
src/ingestion/
  _comum.py                          HTTP com retries, landing zone, watermarks, parsers
  00_setup.py                        schemas, volume, tabelas de controlo (idempotente)
  01_bcb_sgs_ptax.py                 12 séries SGS + PTAX de 10 moedas, todos os boletins
  02_bce_cambio_eur.py               BCE: todas as moedas vs EUR, com atributos SDMX
  03_feriados.py                     BrasilAPI + Nager.Date
  04_ibge_municipios_populacao.py    municípios, UFs, estimativas (SIDRA 6579), Censo 2022 (4709)
  05_meteorologia.py                 arquivo diário/horário, previsão, condições atuais
src/pipeline/
  bronze.py  silver_financeiro.py  silver_territorio.py  silver_meteo.py  gold.py
tests/                               19 testes (parsers + pipeline simulado em Spark local)
```

## O que fica em cada camada

**Landing** (`/Volumes/<catalog>/ops/landing/<fonte>/<dataset>/AAAA/MM/DD/*.json`): cada linha guarda
o registo da API **intacto** (`payload`) num envelope com URL do pedido, instante, lote e contexto.

**Bronze** (17 streaming tables): schema fixo. Uma mudança na API nunca parte a ingestão, e nada se perde.

**Silver** (17 streaming tables, AUTO CDC): colunas tipadas, nomes em português, *expectations*,
deduplicação das sobreposições. Inclui `registo_original` para não perder nenhum campo.

| Tabela | Chave | Destaques |
|---|---|---|
| `bcb_sgs` | série, data | nome, categoria, unidade e periodicidade da série |
| `bcb_ptax_cotacoes` | moeda, hora, boletim | compra, venda, média, spread, paridades USD, abertura/intermédios/fecho |
| `bcb_ptax_moedas` | símbolo | nome, tipo de paridade (A/B) |
| `bce_exr` | moeda, data | taxa por EUR, EUR por unidade, **28 atributos SDMX** |
| `feriados_brasilapi` / `feriados_nager` | data, nome | nacional vs regional, UFs, data fixa vs móvel, nome EN |
| `ibge_municipios` | código | **SCD tipo 2**; micro/mesorregião, região imediata/intermediária, UF, região |
| `ibge_populacao_estimada` / `ibge_censo_2022` | município, ano | valor original do SIDRA preservado |
| `meteo_arquivo_diario` / `_horario` | município, data(/hora) | 20 / 30 variáveis + horas de sol, amplitude térmica |
| `meteo_previsao_diaria` / `_horaria` | município, data(/hora) | emissão mais recente (todas as emissões ficam no bronze) |
| `meteo_atual`, `meteo_locais`, `meteo_unidades` | | condições atuais, coordenadas (centroide IBGE), unidades |

**Gold** (materialized views):

| Tabela | Conteúdo |
|---|---|
| `dim_calendario` | 2010–2030: feriados nacionais e regionais por UF, pontos facultativos, dia útil, **pontes**, nº do dia útil no mês, estação do ano (hemisfério sul) |
| `fato_cambio_ptax_diario` | PTAX de fecho, abertura, variação diária e intradiária, médias móveis 7/30 d, volatilidade 21 d, comparação com a taxa cruzada do BCE |
| `fato_cambio_eur_diario` | todas as moedas do BCE com conversão cruzada para BRL |
| `fato_indicadores_mensal` | IPCA, INPC, IGP-M, IGP-DI com acumulado no ano e em 12 meses; séries diárias resumidas por mês |
| `dim_municipio` | versão atual + população mais recente, Censo 2022, porte, coordenadas, densidade, capital |
| `fato_populacao_municipio` | estimativas e censo por ano, com variação |
| `fato_meteo_diario` | série contínua: histórico + dias recentes + 16 dias de previsão, condição WMO em português, classes de chuva e temperatura, feriado estadual |
| `meteo_condicao_atual`, `dim_condicao_tempo` | tempo agora por município; códigos WMO |

## Como pôr a correr

1. Crie os catálogos `dev_contexto_br` e `prod_contexto_br`, ou altere `catalog` em `databricks.yml`.
2. Com a [Databricks CLI](https://docs.databricks.com/dev-tools/cli/):
   ```bash
   databricks bundle validate
   databricks bundle deploy -t dev
   databricks bundle run -t dev contexto_brasil_job
   ```
   Em alternativa, use **Import** da pasta num Git folder do workspace e faça *Deploy* do bundle a partir da UI.
3. Em `dev` o agendamento fica em pausa (modo *development*); em `prod` fica ativo.

A primeira execução faz o histórico desde 2016 (período do Olist). A meteorologia horária começa em 2024.
Altere `data_inicio`, `data_inicio_horario` e `municipios` (ex.: `capitais,3509502,3170206`) nos parâmetros do job.

## Avisos

- **Open-Meteo**: o plano gratuito é só para uso **não comercial** e tem ~10.000 pedidos/dia. O backfill
  das 27 capitais pode levar 2 dias. Quando o limite é atingido o notebook para sem erro e a execução seguinte
  continua. Para uso comercial ponha a chave num secret scope (`apikey`) e indique-o no widget.
- **Databricks Free Edition**: confirme que o acesso à internet de saída permite estes domínios. Num
  Azure Databricks normal não há essa restrição.
- O BCB SGS e o formato do PTAX, BCE, BrasilAPI, IBGE e Open-Meteo estão cobertos pelos testes com respostas
  no formato documentado. Os endpoints do PTAX, malhas do IBGE e Censo 2022 (SIDRA 4709) não foram chamados
  daqui ao vivo; se algum mudar, a tarefa falha com a mensagem da API e as restantes fontes seguem.
- Para usar a gold no **Microsoft Fabric** sem copiar dados: item *Mirrored Azure Databricks catalog*.

## Testes

```bash
pip install pytest requests pyspark   # pyspark>=4.0 e Java para o teste do pipeline
python -m pytest tests -q
```
