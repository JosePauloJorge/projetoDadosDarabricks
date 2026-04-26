# 🚀 Medallion Architecture Pipeline com Databricks Free Edition + Delta Lake

Projeto de engenharia de dados implementando uma arquitetura **Medallion (Bronze, Silver, Gold)** utilizando **Databricks**, **Delta Lake** e **AWS S3**.

---

## 📌 Objetivo

Demonstrar boas práticas de engenharia de dados com foco em:

* Arquitetura em camadas (Medallion)
* Organização de código (modularização)
* Separação de configuração e lógica
* Observabilidade (logging + alertas)
* Pronto para evolução em ambientes produtivos

---

## 🧱 Arquitetura

```
RAW (CSV)
  ↓
BRONZE (Delta - ingestão)
  ↓
SILVER (limpeza e validação)
  ↓
GOLD (agregação para negócio)
```

---

## ⚙️ Tecnologias utilizadas

* Databricks (Community / Free Edition)
* Apache Spark (PySpark)
* Delta Lake
* AWS S3
* SendGrid (envio de e-mails)
* Python

---

## 📂 Estrutura do projeto

```
config/        → Configurações por ambiente
lib/           → Componentes reutilizáveis (config, logger, notifier)
pipelines/     → Camadas Bronze / Silver / Gold
main_pipeline  → Orquestração do pipeline
```

---

## 🔐 Configuração

Copie o arquivo de exemplo:

```
config/dev.json.example → config/dev.json
```

E preencha:

```json
{
  "paths": {
    "raw": "s3a://seu-bucket/raw/",
    "bronze": "s3a://seu-bucket/bronze/",
    "silver": "s3a://seu-bucket/silver/",
    "gold": "s3a://seu-bucket/gold/"
  },
  "email": {
    "api_key": "SUA_API_KEY_SENDGRID"
  }
}
```

---

## ▶️ Execução

No Databricks:

```python
%run /Workspace/bnptec_project/main_pipeline
```

---

## 📜 Logging

O projeto possui logging estruturado:

```
[DATA] [PIPELINE] [LEVEL] MENSAGEM
```

Exemplo:

```
[2026-04-25 10:00:00] [pipeline_clientes] [INFO] Bronze iniciado
```

---

## 🚨 Alertas

Envio automático de e-mails:

* ✅ Sucesso do pipeline
* ❌ Falha com erro detalhado

---

## 🧠 Boas práticas aplicadas

* Separação de responsabilidades
* Configuração desacoplada
* Reutilização de código
* Tratamento de erros
* Arquitetura escalável

---

## 🔥 Possíveis evoluções

* SCD Tipo 2
* Data Quality / Quarentena
* Auditoria de dados
* Streaming (Auto Loader)
* Orquestração com Jobs
* Integração com BI (Power BI / Tableau)

---

## 📊 Resultado

Este projeto demonstra um pipeline completo de dados seguindo padrões utilizados no mercado.

---

## 👨‍💻 Autor

José Paulo
Engenharia de Dados | Databricks | Spark | AWS

---

