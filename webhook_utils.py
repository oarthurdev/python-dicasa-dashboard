
from libs.supabase_db import SupabaseClient
import logging

logger = logging.getLogger(__name__)

class WebhookMessageManager:
    def __init__(self):
        self.supabase = SupabaseClient()
    
    def get_broker_conversation_history(self, broker_id, days=7):
        """
        Obtém histórico de conversas de um broker nos últimos X dias
        
        Args:
            broker_id (str): ID do broker
            days (int): Número de dias para buscar
            
        Returns:
            dict: Estatísticas e mensagens do broker
        """
        try:
            from datetime import datetime, timedelta
            
            start_date = datetime.now() - timedelta(days=days)
            
            # Buscar mensagens do broker
            messages = self.supabase.client.table("from_webhook").select("*").eq(
                "broker_id", broker_id
            ).gte("inserted_at", start_date.isoformat()).order(
                "inserted_at", desc=True
            ).execute()
            
            if not messages.data:
                return {
                    'broker_id': broker_id,
                    'total_messages': 0,
                    'sent_messages': 0,
                    'received_messages': 0,
                    'conversations': []
                }
            
            # Processar estatísticas
            total_messages = len(messages.data)
            sent_messages = len([m for m in messages.data if m.get('message_type') == 'outgoing'])
            received_messages = len([m for m in messages.data if m.get('message_type') == 'incoming'])
            
            # Agrupar por conversa (chat_id)
            conversations = {}
            for msg in messages.data:
                chat_id = msg.get('chat_id', 'unknown')
                if chat_id not in conversations:
                    conversations[chat_id] = []
                conversations[chat_id].append(msg)
            
            return {
                'broker_id': broker_id,
                'total_messages': total_messages,
                'sent_messages': sent_messages,
                'received_messages': received_messages,
                'response_rate': (sent_messages / max(received_messages, 1)) * 100,
                'conversations': conversations
            }
            
        except Exception as e:
            logger.error(f"Erro ao buscar histórico do broker {broker_id}: {str(e)}")
            return None
    
    def get_lead_messages_with_broker(self, lead_id):
        """
        Obtém todas as mensagens de um lead com informações do broker
        
        Args:
            lead_id (str): ID do lead
            
        Returns:
            dict: Mensagens e informações do broker responsável
        """
        try:
            # Buscar informações do lead
            lead_result = self.supabase.client.table("leads").select(
                "id, nome, responsavel_id"
            ).eq("id", lead_id).execute()
            
            if not lead_result.data:
                return None
            
            lead_data = lead_result.data[0]
            
            # Buscar informações do broker
            broker_result = self.supabase.client.table("brokers").select(
                "id, nome, email"
            ).eq("id", lead_data['responsavel_id']).execute()
            
            broker_data = broker_result.data[0] if broker_result.data else None
            
            # Buscar mensagens do lead
            messages = self.supabase.get_lead_messages(lead_id)
            
            return {
                'lead': lead_data,
                'broker': broker_data,
                'messages': messages,
                'total_messages': len(messages)
            }
            
        except Exception as e:
            logger.error(f"Erro ao buscar mensagens do lead {lead_id}: {str(e)}")
            return None
    
    def get_unlinked_messages(self, limit=100):
        """
        Busca mensagens que ainda não foram vinculadas a brokers
        
        Args:
            limit (int): Limite de mensagens
            
        Returns:
            list: Mensagens não vinculadas
        """
        try:
            result = self.supabase.client.table("from_webhook").select("*").is_(
                "broker_id", "null"
            ).order("inserted_at", desc=True).limit(limit).execute()
            
            return result.data if result.data else []
            
        except Exception as e:
            logger.error(f"Erro ao buscar mensagens não vinculadas: {str(e)}")
            return []
    
    def reprocess_unlinked_messages(self):
        """
        Reprocessa mensagens que não foram vinculadas a brokers
        
        Returns:
            int: Número de mensagens reprocessadas
        """
        try:
            unlinked = self.get_unlinked_messages()
            reprocessed = 0
            
            for message in unlinked:
                # Tentar vincular novamente
                linked = self.supabase.link_webhook_message_to_broker(message)
                
                if linked.get('broker_id'):
                    # Atualizar no banco
                    self.supabase.client.table("from_webhook").update({
                        'broker_id': linked['broker_id'],
                        'lead_id': linked.get('lead_id')
                    }).eq("id", message['id']).execute()
                    reprocessed += 1
            
            logger.info(f"Reprocessadas {reprocessed} mensagens de {len(unlinked)} não vinculadas")
            return reprocessed
            
        except Exception as e:
            logger.error(f"Erro ao reprocessar mensagens: {str(e)}")
            return 0
