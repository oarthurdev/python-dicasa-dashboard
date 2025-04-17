-- Criar tabela para armazenar dados dos corretores
CREATE TABLE IF NOT EXISTS brokers (
    id bigint PRIMARY KEY,
    nome text NOT NULL,
    email text,
    foto_url text,
    cargo text,
    criado_em timestamp,
    updated_at timestamp DEFAULT now()
);

-- Criar tabela para armazenar dados dos leads
CREATE TABLE IF NOT EXISTS leads (
    id bigint PRIMARY KEY,
    nome text NOT NULL,
    responsavel_id bigint,
    contato_nome text,
    valor numeric,
    status_id bigint,
    pipeline_id bigint,
    etapa text,
    criado_em timestamp,
    atualizado_em timestamp,
    fechado boolean DEFAULT false,
    status text,
    updated_at timestamp DEFAULT now(),
    FOREIGN KEY (responsavel_id) REFERENCES brokers (id) ON DELETE SET NULL
);

-- Criar tabela para armazenar dados das atividades
CREATE TABLE IF NOT EXISTS activities (
    id text PRIMARY KEY,
    lead_id bigint,
    user_id bigint,
    tipo text,
    valor_anterior text,
    valor_novo text,
    criado_em timestamp,
    dia_semana text,
    hora integer,
    updated_at timestamp DEFAULT now(),
    FOREIGN KEY (lead_id) REFERENCES leads (id) ON DELETE CASCADE,
    FOREIGN KEY (user_id) REFERENCES brokers (id) ON DELETE SET NULL
);

-- Criar tabela para armazenar a pontuação dos corretores
CREATE TABLE IF NOT EXISTS broker_points (
    id bigint PRIMARY KEY,
    nome text NOT NULL,
    pontos integer DEFAULT 0,
    leads_respondidos_1h integer DEFAULT 0,
    leads_visitados integer DEFAULT 0,
    propostas_enviadas integer DEFAULT 0,
    vendas_realizadas integer DEFAULT 0,
    leads_atualizados_mesmo_dia integer DEFAULT 0,
    feedbacks_positivos integer DEFAULT 0,
    leads_sem_interacao_24h integer DEFAULT 0,
    leads_respondidos_apos_18h integer DEFAULT 0,
    leads_tempo_resposta_acima_12h integer DEFAULT 0,
    leads_5_dias_sem_mudanca integer DEFAULT 0,
    updated_at timestamp DEFAULT now(),
    FOREIGN KEY (id) REFERENCES brokers (id) ON DELETE CASCADE
);

-- Criar políticas RLS (Row Level Security) para cada tabela
-- Isso permite que o cliente anônimo possa ler e escrever nas tabelas

-- Política para brokers
ALTER TABLE brokers ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Anônimo pode ler brokers" ON brokers
    FOR SELECT USING (true);
CREATE POLICY "Anônimo pode inserir brokers" ON brokers
    FOR INSERT WITH CHECK (true);
CREATE POLICY "Anônimo pode atualizar brokers" ON brokers
    FOR UPDATE USING (true) WITH CHECK (true);
CREATE POLICY "Anônimo pode excluir brokers" ON brokers
    FOR DELETE USING (true);

-- Política para leads
ALTER TABLE leads ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Anônimo pode ler leads" ON leads
    FOR SELECT USING (true);
CREATE POLICY "Anônimo pode inserir leads" ON leads
    FOR INSERT WITH CHECK (true);
CREATE POLICY "Anônimo pode atualizar leads" ON leads
    FOR UPDATE USING (true) WITH CHECK (true);
CREATE POLICY "Anônimo pode excluir leads" ON leads
    FOR DELETE USING (true);

-- Política para activities
ALTER TABLE activities ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Anônimo pode ler activities" ON activities
    FOR SELECT USING (true);
CREATE POLICY "Anônimo pode inserir activities" ON activities
    FOR INSERT WITH CHECK (true);
CREATE POLICY "Anônimo pode atualizar activities" ON activities
    FOR UPDATE USING (true) WITH CHECK (true);
CREATE POLICY "Anônimo pode excluir activities" ON activities
    FOR DELETE USING (true);

-- Política para broker_points
ALTER TABLE broker_points ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Anônimo pode ler broker_points" ON broker_points
    FOR SELECT USING (true);
CREATE POLICY "Anônimo pode inserir broker_points" ON broker_points
    FOR INSERT WITH CHECK (true);
CREATE POLICY "Anônimo pode atualizar broker_points" ON broker_points
    FOR UPDATE USING (true) WITH CHECK (true);
CREATE POLICY "Anônimo pode excluir broker_points" ON broker_points
    FOR DELETE USING (true);