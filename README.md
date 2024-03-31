### Atividade Datum

# Escolha um conjunto de dados do Kaggle relacionado a vendas. 
  - Certifique-se de que o conjunto de dados inclui informações como datas, produtos, quantidades vendidas, etc.
  - Projeto de Engenharia de Dados:

## 1. Ingestão e Carregamento de Dados:
  - Carregue o conjunto de dados no Databricks.
  - Explore o esquema dos dados e faça ajustes conforme necessário.

## 2. Transformações de Dados:
  - Realize transformações necessárias, como tratamento de valores nulos, conversões de tipos, etc.
  - Adicione uma coluna calculada, por exemplo, o valor total de cada transação.
  - Agregue os dados para obter estatísticas de vendas, por exemplo, o total de vendas por produto ou por categoria.
  - Introduza uma regra mais complexa, como identificar padrões de comportamento de compra ao longo do tempo ou criar categorias personalizadas de produtos com base em determinados critérios.

### 3. Saída em Parquet e Delta:
  - Grave os dados transformados e agregados em um formato Parquet para persistência eficiente.
  - Grave os mesmos dados em formato Delta Lake para aproveitar as funcionalidades de versionamento e transações ACID. 

## 5.1- Exploração Adicional (Opcional):
  - Execute consultas exploratórias para entender melhor os dados e validar as transformações.
  - Crie visualizações ou relatórios para comunicar insights.
  - Agende o notebook para execução automática em intervalos regulares para garantir a atualização contínua dos dados.


# Desenvolvimento:
  - Utilizado o dataset 'Bike Store' do Kaggle disposto no link <https://www.kaggle.com/datasets/dillonmyrick/bike-store-sample-database> pois o mesmo cumpre os requisitos.
  
  - Cópia dos dados diariamente utilizando marca d'agua. 

  - Utilizado a arquitetura Medalhão para organizar as pastas de trabalho.
    - Cada nível tem sua pasta;
    - Além disso criei uma pasta 'Utils' para eventuais classes e funções genéricas necessárias (nesse caso não foi preciso);
    - Dentro da pasta que identintifica o nível da arquitetura medalhão, existe uma pasta referente ao projeto, para organização. Ficando da seguinte maneira:
      - <i>'Workspace / Nivel / Kaggle /'</i>
    - Dados foram salvos no DBFS por serem pequenos e possuírem certa organização (Em caso de maior quantidade o ideial seria utilizar o BlogStorage);
  
  - Tratamentos realizados (Em todos os arquivos que compõem o dataset):
    - Definidos os tipos corretos de dado;
    - Removidos valores com determinadas colunas que não deveriam ser nuláveis;
    - Valores quantitativos e montatários negativos;
    - Adicionada coluna calculada de percentual de desconto realizada no produto da venda;
    - Calculado diferença entre data de compra e entrega;
    - Definido e formatado tipo correto de dado data
  
  - Visões criadas:
    - Total de vendas por loja;
    - Total de Ano de cada categoria;
    - Vendas mensais de cada vendedor;
    - Vendas mensais de cada loja;
  
  - Organização do DBFS:
    - Pastas <b>Bronze</b>, <b>Silver</b> e <b>Gold</b>
      - Bronze: <i>'bronze/bike_store_sample_database/'</i>
      - Silver: <i>'silver/delta/bike_store_sample_database/'</i>
      - Gold: Parquet: <i>'/dbfs/gold/parquet/bike_store_sample_database'</i> e Delta: <i>'gold/delta/bike_store_sample_database/'</i>
  
  - Organização do WorkFlow (Disponibilizado link no Drive):
    - Foi pensado em somente processar o próximo notebook, caso os dados relacionados estiverem processados com sucesso já;
    - Separado em níveis:
      - 1º Nível: Execução somente do notebook Bronze que busca os dados;
      - 2º Nível: Execução dos notebooks Silver, encadeados de acordo com a dependência de informações;
        - Neste nível temos dependência de execução, onde quando um silver detém dados relacionados de outro, o segundo somente executa em caso de sucesso dos dados relacionados.
      - 3º Nível: Notebooks Gold;

