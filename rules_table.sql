
CREATE TABLE IF NOT EXISTS rules (
    id SERIAL PRIMARY KEY,
    nome VARCHAR(255) NOT NULL,
    pontos INTEGER NOT NULL,
    coluna_nome VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Add RLS policies
ALTER TABLE rules ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Anônimo pode ler rules" ON rules
    FOR SELECT USING (true);
CREATE POLICY "Anônimo pode inserir rules" ON rules
    FOR INSERT WITH CHECK (true);
CREATE POLICY "Anônimo pode atualizar rules" ON rules
    FOR UPDATE USING (true) WITH CHECK (true);
CREATE POLICY "Anônimo pode excluir rules" ON rules
    FOR DELETE USING (true);
-- Create rules table
CREATE TABLE IF NOT EXISTS rules (
    id SERIAL PRIMARY KEY,
    nome TEXT NOT NULL,
    pontos INTEGER NOT NULL,
    coluna_nome TEXT NOT NULL UNIQUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Enable RLS
ALTER TABLE rules ENABLE ROW LEVEL SECURITY;

-- Create RLS policies
CREATE POLICY "Enable all access for authenticated users" ON rules
    FOR ALL USING (auth.role() = 'authenticated')
    WITH CHECK (auth.role() = 'authenticated');
