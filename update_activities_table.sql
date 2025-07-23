
-- Script para atualizar a estrutura da tabela activities
-- Adicionar novos campos para melhor rastreamento dos eventos da Kommo

ALTER TABLE activities 
ADD COLUMN IF NOT EXISTS status_anterior INTEGER,
ADD COLUMN IF NOT EXISTS status_novo INTEGER,
ADD COLUMN IF NOT EXISTS texto_mensagem TEXT,
ADD COLUMN IF NOT EXISTS fonte_mensagem VARCHAR(50),
ADD COLUMN IF NOT EXISTS texto_tarefa TEXT,
ADD COLUMN IF NOT EXISTS tipo_tarefa INTEGER,
ADD COLUMN IF NOT EXISTS entity_type VARCHAR(50),
ADD COLUMN IF NOT EXISTS entity_id INTEGER;

-- Criar índices para melhor performance
CREATE INDEX IF NOT EXISTS idx_activities_tipo ON activities(tipo);
CREATE INDEX IF NOT EXISTS idx_activities_lead_id ON activities(lead_id);
CREATE INDEX IF NOT EXISTS idx_activities_user_id ON activities(user_id);
CREATE INDEX IF NOT EXISTS idx_activities_criado_em ON activities(criado_em);
CREATE INDEX IF NOT EXISTS idx_activities_company_id ON activities(company_id);

-- Comentários para documentar os campos
COMMENT ON COLUMN activities.status_anterior IS 'Status anterior do lead (para mudanças de status)';
COMMENT ON COLUMN activities.status_novo IS 'Novo status do lead (para mudanças de status)';
COMMENT ON COLUMN activities.texto_mensagem IS 'Texto da mensagem (para mensagens enviadas/recebidas)';
COMMENT ON COLUMN activities.fonte_mensagem IS 'Fonte da mensagem (whatsapp, telegram, etc.)';
COMMENT ON COLUMN activities.texto_tarefa IS 'Texto da tarefa (para tarefas criadas/concluídas)';
COMMENT ON COLUMN activities.tipo_tarefa IS 'Tipo da tarefa (ID do tipo na Kommo)';
COMMENT ON COLUMN activities.entity_type IS 'Tipo da entidade relacionada (lead, contact, etc.)';
COMMENT ON COLUMN activities.entity_id IS 'ID da entidade relacionada';
