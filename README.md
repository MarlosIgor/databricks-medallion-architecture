Aqui está a versão atualizada do `README.md`, incluindo a **Load Gold Delta Incremental**:

# Databricks Lakehouse Pipelines

## Descrição

Este projeto demonstra a implementação de pipelines de dados em uma arquitetura **Lakehouse** utilizando **Databricks**, **PySpark** e **Delta Lake**. Através de uma abordagem **Medallion Architecture**, são apresentadas boas práticas para otimização e manutenção de clusters e dados, incluindo operações como **Vacuum**, **Optimize**, **ZORDER**, **time travel** e **manipulação de dados** com PySpark. Também implementa **cargas incrementais** na camada Gold utilizando Delta Lake.

## Tecnologias Utilizadas

- **Databricks**: Plataforma para processamento em nuvem.
- **PySpark**: Biblioteca para processamento distribuído de dados em larga escala.
- **Delta Lake**: Camada de armazenamento otimizada que oferece suporte a transações ACID.
- **Arquitetura Lakehouse**: Combinação dos benefícios de Data Lakes e Data Warehouses.
- **Medallion Architecture**: Arquitetura de dados em camadas (Bronze, Silver e Gold) para otimizar e organizar dados em escala.

## Funcionalidades

1. **Configuração e Gerenciamento de Clusters no Databricks**: 
   - Criação e configuração de clusters no Databricks.
   - Execução de consultas distribuídas e manipulação de dados com PySpark.
   
2. **Manipulação e Transformação de Dados com PySpark**: 
   - Operações de ETL utilizando PySpark para leitura e transformação de dados.
   - Criação de tabelas dimensionais e fato com chaves surrogate e atualizações incrementais.

3. **Implementação de Pipelines de Dados com Delta Lake**: 
   - Criação de tabelas Delta para armazenamento otimizado.
   - Particionamento e compactação de dados para melhoria de desempenho.

4. **Arquitetura Medallion (Bronze, Silver e Gold)**: 
   - Pipeline em várias camadas para ingestão e transformação de dados.
   - Camada **Bronze**: Dados brutos.
   - Camada **Silver**: Dados limpos e transformados.
   - Camada **Gold**: Dados prontos para análise.

5. **Carga Gold Delta Incremental**:
   - Implementação de rotinas incrementais na camada Gold, otimizando o carregamento apenas com novos dados.
   - Verificação de novos registros e atualização das tabelas Delta.

6. **Rotinas de Manutenção e Otimização**:
   - **Vacuum**: Remoção de arquivos antigos não referenciados.
   - **Optimize e ZORDER**: Reorganização e compactação de arquivos para melhor performance.
   - **Repartition**: Redistribuição dos dados para clusters balanceados.

## Estrutura do Projeto

```bash
databricks-lakehouse-pipelines/
│
├── notebooks/                  # Notebooks do Databricks com os códigos PySpark
│   ├── ingestao_bronze.py       # Pipeline de ingestão de dados para camada Bronze
│   ├── transformacao_silver.py  # Transformação e limpeza de dados para camada Silver
│   └── fato_vendas_gold.py      # Criação de tabelas fato na camada Gold
│   └── load_gold_incremental.py # Pipeline de carga incremental para a camada Gold
│
├── delta-tables/                # Tabelas Delta utilizadas no projeto
│   ├── dim_produto/             # Tabela dimensão produto
│   └── fato_vendas/             # Tabela fato vendas
│
└── README.md                    # Documentação do projeto
```

## Como Executar

### Pré-requisitos
- Conta no **Databricks**.
- **Clusters** configurados no Databricks.
- Familiaridade com **PySpark** e **Delta Lake**.

## Licença

Este projeto está sob a licença MIT. Veja o arquivo [LICENSE](LICENSE) para mais detalhes.
