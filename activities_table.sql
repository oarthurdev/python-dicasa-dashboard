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
    FOREIGN K
