
-- Estrutura da tabela monthly_logs
CREATE TABLE monthly_logs (
    id SERIAL PRIMARY KEY,
    month_start TIMESTAMP NOT NULL,
    month_end TIMESTAMP NOT NULL,
    company_id TEXT NOT NULL,
    total_leads INTEGER DEFAULT 0,
    total_points INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Índices para melhorar performance
CREATE INDEX idx_monthly_logs_company_id ON monthly_logs(company_id);
CREATE INDEX idx_monthly_logs_month_start ON monthly_logs(month_start);
CREATE INDEX idx_monthly_logs_created_at ON monthly_logs(created_at);

-- Comentários explicativos
COMMENT ON TABLE monthly_logs IS 'Tabela para armazenar logs mensais de dados das empresas';
COMMENT ON COLUMN monthly_logs.id IS 'ID único do log mensal';
COMMENT ON COLUMN monthly_logs.month_start IS 'Data de início do mês';
COMMENT ON COLUMN monthly_logs.month_end IS 'Data de fim do mês';
COMMENT ON COLUMN monthly_logs.company_id IS 'ID da empresa';
COMMENT ON COLUMN monthly_logs.total_leads IS 'Total de leads do mês';
COMMENT ON COLUMN monthly_logs.total_points IS 'Total de pontos do mês';
COMMENT ON COLUMN monthly_logs.created_at IS 'Data de criação do registro';
COMMENT ON COLUMN monthly_logs.updated_at IS 'Data de última atualização';
