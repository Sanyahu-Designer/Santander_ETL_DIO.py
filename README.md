# 🏦 Santander ETL 2025 - JSONPlaceholder Edition

Projeto ETL moderno usando JSONPlaceholder API e OpenAI para gerar mensagens personalizadas de investimentos.

## ✨ Funcionalidades

- ✅ **EXTRACT**: Carrega usuários de CSV + JSONPlaceholder API
- ✅ **TRANSFORM**: Gera mensagens personalizadas com OpenAI GPT
- ✅ **LOAD**: Salva resultados em arquivos JSON locais
- ✅ **Relatórios**: Gera analytics do processamento

## 🚀 Como Usar

1. **Instale as dependências:**
```bash
pip install -r requirements.txt
```
2. Configure sua OpenAI API Key:
```bash
# No arquivo santander_etl_moderno.py
OPENAI_API_KEY = "sua-chave-real-aqui"
```
3. Execute o pipeline:
```bash
python santander_ETL_DIO.py
```
📊 Estrutura de Saída
```bash
project/
├── user_updates/
│   ├── user_1_20241205_143022.json
│   ├── user_2_20241205_143022.json
│   └── ...
├── etl_report.json
└── processing_log.txt
```
🛠️ Tecnologias

Python 3.8+
JSONPlaceholder API
OpenAI GPT
Pandas
Requests

📝 Notas
API JSONPlaceholder fornece dados de usuários fake
Mensagens são geradas por IA com contexto personalizado
Resultados salvos localmente para demonstração
Fácil adaptação para APIs reais
