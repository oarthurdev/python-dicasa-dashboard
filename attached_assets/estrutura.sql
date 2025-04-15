
--
-- Name: activities; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.activities (
    id text NOT NULL,
    lead_id bigint,
    user_id bigint,
    tipo text,
    valor_anterior text,
    valor_novo text,
    criado_em timestamp without time zone,
    dia_semana text,
    hora integer,
    updated_at timestamp without time zone DEFAULT now()
);


--
-- Name: broker_points; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.broker_points (
    id bigint NOT NULL,
    nome text NOT NULL,
    pontos integer DEFAULT 0,
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
    leads_sem_interacao_24h integer DEFAULT 0,
    leads_ignorados_48h integer DEFAULT 0,
    leads_com_reclamacao integer DEFAULT 0,
    leads_perdidos integer DEFAULT 0,
    leads_respondidos_apos_18h integer DEFAULT 0,
    leads_tempo_resposta_acima_12h integer DEFAULT 0,
    leads_5_dias_sem_mudanca integer DEFAULT 0,
    updated_at timestamp without time zone DEFAULT now()
);


--
-- Name: brokers; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.brokers (
    id bigint NOT NULL,
    nome text NOT NULL,
    email text,
    foto_url text,
    cargo text,
    criado_em timestamp without time zone,
    updated_at timestamp without time zone DEFAULT now()
);


--
-- Name: leads; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.leads (
    id bigint NOT NULL,
    nome text NOT NULL,
    responsavel_id bigint,
    contato_nome text,
    valor numeric,
    status_id bigint,
    pipeline_id bigint,
    etapa text,
    criado_em timestamp without time zone,
    atualizado_em timestamp without time zone,
    fechado boolean DEFAULT false,
    status text,
    updated_at timestamp without time zone DEFAULT now()
);