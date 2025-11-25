import pandas as pd
import requests
import json
import openai
import random
from datetime import datetime
from typing import List, Dict, Optional

class SantanderETL2025:
    """
    ETL Moderno para Santander Dev Week usando JSONPlaceholder
    Versão atualizada para 2025
    """
    
    def __init__(self, openai_api_key: str):
        self.openai_api_key = openai_api_key
        self.users_api_url = "https://jsonplaceholder.typicode.com/users"
        
    def extract_users_from_csv(self, csv_path: str) -> List[int]:
        """
        EXTRACT: Extrai IDs de usuários do arquivo CSV
        """
        try:
            df = pd.read_csv(csv_path)
            user_ids = df['UserID'].tolist()
            print(f"✅ EXTRACT: {len(user_ids)} IDs extraídos do CSV")
            return user_ids
        except Exception as e:
            print(f"❌ Erro na extração: {e}")
            return []
    
    def get_user_data(self, user_id: int) -> Optional[Dict]:
        """
        EXTRACT: Obtém dados do usuário da API JSONPlaceholder
        """
        try:
            response = requests.get(f"{self.users_api_url}/{user_id}")
            
            if response.status_code == 200:
                api_data = response.json()
                
                # Transforma dados da API para nosso formato bancário
                user_data = {
                    "id": api_data["id"],
                    "name": api_data["name"],
                    "username": api_data["username"],
                    "email": api_data["email"],
                    "phone": api_data["phone"],
                    "website": api_data["website"],
                    "address": api_data["address"],
                    "company": api_data["company"],
                    # Dados bancários simulados
                    "account": {
                        "number": f"001{api_data['id']:04d}",
                        "agency": "0001",
                        "balance": round(random.uniform(1000, 50000), 2),
                        "limit": 5000.00
                    },
                    "news": []  # Array para nossas mensagens
                }
                
                print(f"📥 Usuário {user_id}: {api_data['name']} carregado")
                return user_data
            else:
                print(f"⚠️ Usuário {user_id} não encontrado na API")
                return None
                
        except requests.exceptions.RequestException as e:
            print(f"❌ Erro de conexão ao buscar usuário {user_id}: {e}")
            return None
    
    def generate_ai_news(self, user: Dict) -> str:
        """
        TRANSFORM: Gera mensagem personalizada usando OpenAI
        """
        try:
            # Configuração da OpenAI (versão mais recente)
            client = openai.OpenAI(api_key=self.openai_api_key)
            
            # Contexto mais rico para a IA
            user_context = f"""
            Cliente: {user['name']}
            Email: {user['email']}
            Empresa: {user['company']['name']}
            Saldo atual: R$ {user['account']['balance']:,.2f}
            """
            
            response = client.chat.completions.create(
                model="gpt-3.5-turbo",  # Pode usar gpt-4 se disponível
                messages=[
                    {
                        "role": "system",
                        "content": """Você é um consultor financeiro especializado do Santander. 
                        Crie mensagens personalizadas e motivadoras sobre investimentos. 
                        Seja direto, pessoal e focado no futuro financeiro do cliente.
                        Máximo de 120 caracteres."""
                    },
                    {
                        "role": "user",
                        "content": f"""Crie uma mensagem personalizada para {user['name']} 
                        sobre a importância dos investimentos. Use estas informações:
                        {user_context}
                        
                        Mensagem deve ser curta, impactante e personalizada."""
                    }
                ],
                max_tokens=80,
                temperature=0.8
            )
            
            message = response.choices[0].message.content.strip()
            print(f"🤖 IA: Mensagem gerada para {user['name']}")
            return message
            
        except Exception as e:
            print(f"❌ Erro ao gerar mensagem com IA: {e}")
            # Mensagem fallback personalizada
            return f"{user['name']}, invista hoje para um futuro financeiro mais seguro e próspero!"
    
    def update_user_data(self, user: Dict, message: str) -> bool:
        """
        LOAD: Atualiza dados do usuário (simulado - salva em arquivo)
        """
        try:
            # Cria objeto de notícia
            news_item = {
                "id": len(user['news']) + 1,
                "date": datetime.now().isoformat(),
                "icon": "https://cdn-icons-png.flaticon.com/512/3135/3135679.png",
                "description": message,
                "category": "investment_advice",
                "read": False
            }
            
            # Adiciona à lista de news
            user['news'].append(news_item)
            
            # Simula update na API - salva em arquivo local
            self._save_user_update(user)
            
            print(f"💾 LOAD: Dados de {user['name']} atualizados")
            return True
            
        except Exception as e:
            print(f"❌ Erro no carregamento: {e}")
            return False
    
    def _save_user_update(self, user: Dict):
        """
        Salva dados do usuário em arquivo JSON para simular persistência
        """
        filename = f"user_updates/user_{user['id']}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        # Garante que o diretório existe
        import os
        os.makedirs('user_updates', exist_ok=True)
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(user, f, indent=2, ensure_ascii=False)
    
    def generate_report(self, users: List[Dict]):
        """
        Gera relatório final do processamento
        """
        report = {
            "timestamp": datetime.now().isoformat(),
            "total_users_processed": len(users),
            "users_with_news": sum(1 for user in users if user.get('news')),
            "total_messages_generated": sum(len(user.get('news', [])) for user in users),
            "processing_summary": [
                {
                    "user_id": user["id"],
                    "user_name": user["name"],
                    "messages_count": len(user.get('news', [])),
                    "last_message": user['news'][-1]['description'] if user.get('news') else "N/A"
                }
                for user in users
            ]
        }
        
        with open('etl_report.json', 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        print(f"\n📊 RELATÓRIO: Processados {len(users)} usuários")
        print(f"💌 Total de mensagens: {report['total_messages_generated']}")
    
    def run_etl_pipeline(self, csv_path: str):
        """
        Executa o pipeline ETL completo
        """
        print("=" * 60)
        print("🚀 SANTANDER ETL 2025 - INICIANDO PIPELINE")
        print("=" * 60)
        
        # EXTRACT
        print("\n📥 FASE 1: EXTRACT")
        user_ids = self.extract_users_from_csv(csv_path)
        
        if not user_ids:
            print("❌ Nenhum ID encontrado para processar")
            return
        
        users = []
        for user_id in user_ids:
            user_data = self.get_user_data(user_id)
            if user_data:
                users.append(user_data)
        
        print(f"✅ EXTRACT concluído: {len(users)} usuários carregados")
        
        # TRANSFORM
        print("\n🔄 FASE 2: TRANSFORM")
        processed_users = []
        
        for user in users:
            print(f"\n🔄 Processando: {user['name']}")
            
            # Gera mensagem personalizada com IA
            ai_message = self.generate_ai_news(user)
            print(f"💡 Mensagem: {ai_message}")
            
            user['ai_generated_message'] = ai_message
            processed_users.append(user)
        
        # LOAD
        print("\n📤 FASE 3: LOAD")
        success_count = 0
        
        for user in processed_users:
            success = self.update_user_data(user, user['ai_generated_message'])
            if success:
                success_count += 1
        
        # RELATÓRIO
        print("\n📊 FASE 4: RELATÓRIO")
        self.generate_report(processed_users)
        
        print("\n" + "=" * 60)
        print("🎉 PIPELINE ETL CONCLUÍDO COM SUCESSO!")
        print(f"✅ {success_count}/{len(processed_users)} usuários processados")
        print("📁 Verifique os arquivos em 'user_updates/' para os resultados")
        print("=" * 60)
        
        return processed_users

def main():
    """
    Função principal - exemplo de uso
    """
    # Configuração - use variáveis de ambiente na prática!
    OPENAI_API_KEY = "sua_chave_openai_aqui"  # Substitua pela sua chave
    
    if OPENAI_API_KEY == "sua_chave_openai_aqui":
        print("❌ Configure sua OpenAI API Key no código!")
        return
    
    # Inicializa e executa ETL
    etl = SantanderETL2025(OPENAI_API_KEY)
    
    try:
        results = etl.run_etl_pipeline('SDW2023.csv')
        
        # Exibe resumo bonito
        if results:
            print("\n👥 RESUMO DOS CLIENTES PROCESSADOS:")
            for user in results:
                print(f"   • {user['name']}: {user['ai_generated_message']}")
                
    except Exception as e:
        print(f"❌ Erro no pipeline: {e}")

if __name__ == "__main__":
    main()
