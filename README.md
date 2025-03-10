# 📌 Projeto

Este projeto tem como objetivo processar e analisar os dados do **Censo Escolar**, garantindo qualidade e consistência por meio de um pipeline estruturado.  

## ✅ Testes  

O projeto inclui **testes unitários** para cada etapa do processo, assegurando a confiabilidade das transformações aplicadas:  

- 🔍 **Testes de extração**: Garantem a correta obtenção dos dados brutos.  
- 🔄 **Testes de transformação**: Validam as alterações e limpezas aplicadas aos dados.  
- 💾 **Testes de carregamento**: Verificam a integridade dos dados ao serem armazenados.  
- 🔗 **Testes de integração**: Garantem que os componentes do pipeline funcionam corretamente juntos.  

## 📁 Estrutura de Dados  

Os dados passam por diferentes estágios de processamento:  

- 📂 **Raw**: Contém os dados originais extraídos do **Censo Escolar**.  
- 🛠️ **Processed**: Dados após transformações iniciais, como limpeza e padronização.  
- 📊 **Final**: Dados prontos para análise e geração de insights.  

# Censo Escolar ETL

## Estrutura de Diretórios

O projeto espera a seguinte estrutura de diretórios:

```
pjtCensoEscolar/
├── data/
│   ├── raw/           # Arquivos CSV originais
│   │   └── escolas.csv
│   └── processed/     # Arquivos processados
├── src/               # Código fonte
└── main.py
```

## Configuração

1. Crie os diretórios necessários:
   ```bash
   mkdir -p data/raw data/processed
   ```

2. Coloque seu arquivo `escolas.csv` no diretório `data/raw/`

3. Execute o script principal:
   ```bash
   python main.py
   ```

## Formato dos Dados

O arquivo `escolas.csv` deve estar no diretório `data/raw/` e conter as seguintes colunas:
- IN_AGUA_POTAVEL
- IN_ENERGIA_INEXISTENTE
- TP_SITUACAO_FUNCIONAMENTO
- TP_DEPENDENCIA
- (outras colunas conforme necessário)
