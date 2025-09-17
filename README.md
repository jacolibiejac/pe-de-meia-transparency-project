# 📊 Pé-de-Meia Transparency Project

Este projeto automatiza a coleta e análise de dados do **Programa Pé-de-Meia** no [Portal da Transparência](https://portaldatransparencia.gov.br/).  
O objetivo é construir uma **pipeline de dados pública e reprodutível**, utilizando **Python**, **Selenium** e **Pandas**, para consolidar milhões de registros em planilhas e permitir análises abertas.

---

## 🚀 Tecnologias utilizadas
- **Python 3.10+**
- **Selenium** → automação de navegador para exportação em massa  
- **Requests** → requisições HTTP / APIs  
- **Pandas** → tratamento e consolidação dos dados  
- **Jupyter Notebook** → análises gráficas e exploratórias  

---

## 📂 Estrutura do Projeto

```bash
pe-de-meia-transparency-project/
│── data/                         # CSVs brutos e consolidados
│   ├── dados_portal_transparencia.csv
│   ├── dados_portal_transparencia_completo.csv
│
│── scripts/                      # Scripts principais (scraping/API)
│   ├── coletar_pe_de_meia_completo.py
│   ├── scraper_portal_transparencia.py
│   ├── scraper_simples.py
│   └── combinar_csvs.py
│
│── notebooks/
│   └── exploracao_dados.ipynb    # Notebook de análise
│
│── requirements.txt              # Dependências do projeto
│── README.md                     # Documentação principal
│── LICENSE                       # Licença do projeto (MIT)
