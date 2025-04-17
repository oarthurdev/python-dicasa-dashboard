
CREATE TABLE IF NOT EXISTS broker_points (
    id bigint PRIMARY KEY,
    nome text NOT NULL,
    pontos integer DEFAULT 0,
    -- Positive metrics
    leads_respondidos_1h integer DEFAULT 0,
    leads_visitados integer DEFAULT 0,
    propostas_enviadas integer DEFAULT 0,
    vendas_realizadas integer DEFAULT 0,
    leads_atualizados_mesmo_dia integer DEFAULT 0,
    feedbacks_positivos integer DEFAULT 0,
    resposta_rapida_3h integer DEFAULT 0,
    todos_leads_respondidos integer DEFAULT 0,
    cadastro_completo integer DEFAULT 0,
    acompanhamento_pos_venda integer DEFAULT 0,
    
    -- Negative metrics
    leads_sem_interacao_24h integer DEFAULT 0,
    leads_ignorados_48h integer DEFAULT 0,
    leads_com_reclamacao integer DEFAULT 0,
    leads_perdidos integer DEFAULT 0,
    
    -- Alert metrics (no points impact)
    leads_respondidos_apos_18h integer DEFAULT 0,
    leads_tempo_resposta_acima_12h integer DEFAULT 0,
    leads_5_dias_sem_mudanca integer DEFAULT 0,
    
    updated_at timestamp DEFAULT now(),
    FOREIGN KEY (id) REFERENCES brokers (id) ON DELETE CASCADE
);

-- Add RLS (Row Level Security) policy
ALTER TABLE broker_points ENABLE ROW LEVEL SECURITY;

-- Create policies for table access
CREATE POLICY "Público pode ler broker_points" ON broker_points
    FOR SELECT USING (true);
CREATE POLICY "Público pode inserir broker_points" ON broker_points
    FOR INSERT WITH CHECK (true);
CREATE POLICY "Público pode atualizar broker_points" ON broker_points
    FOR UPDATE USING (true) WITH CHECK (true);
CREATE POLICY "Público pode excluir broker_points" ON broker_points
    FOR DELETE USING (true);
