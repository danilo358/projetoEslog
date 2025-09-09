CREATE TABLE IF NOT EXISTS cadastro.veiculo (
  placa TEXT PRIMARY KEY,
  empresa TEXT,
  descricao TEXT,
  capacidade_tanque_litros NUMERIC,
  ativo BOOLEAN NOT NULL DEFAULT TRUE,
  criado_em TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS rastreio.posicao (
  id_position BIGINT PRIMARY KEY,
  placa TEXT NOT NULL REFERENCES cadastro.veiculo(placa),
  id_event INT,
  ignicao BOOLEAN,
  valid_gps BOOLEAN,
  data_evento TIMESTAMPTZ NOT NULL,
  data_atualizacao TIMESTAMPTZ,
  latitude DOUBLE PRECISION NOT NULL,
  longitude DOUBLE PRECISION NOT NULL,
  geom geometry(Point, 4326)
  GENERATED ALWAYS AS (ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)) STORED,
  inputs JSONB,
  outputs JSONB,
  telemetria JSONB,
  nivel_tanque_percent NUMERIC,
  raw JSONB
);
CREATE INDEX IF NOT EXISTS idx_posicao_placa_evento
  ON rastreio.posicao (placa, data_evento DESC);
  CREATE INDEX IF NOT EXISTS idx_posicao_geom
  ON rastreio.posicao USING GIST (geom);

CREATE TABLE IF NOT EXISTS operacao.evento_tanque (
  id_evento BIGSERIAL PRIMARY KEY,
  placa TEXT NOT NULL REFERENCES cadastro.veiculo(placa),
  tipo TEXT NOT NULL CHECK (tipo IN ('COLETA','DESCARGA')),
  data_hora TIMESTAMPTZ NOT NULL,
  latitude DOUBLE PRECISION NOT NULL,
  longitude DOUBLE PRECISION NOT NULL,
  geom geometry(Point, 4326)
  GENERATED ALWAYS AS (ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)) STORED,
  variacao_percent NUMERIC,
  nivel_anterior NUMERIC,
  nivel_atual NUMERIC,
  origem_posicao BIGINT UNIQUE
);
CREATE INDEX IF NOT EXISTS idx_evento_tanque_placa_data
  ON operacao.evento_tanque (placa, data_hora DESC);

CREATE INDEX IF NOT EXISTS idx_evento_tanque_earth
  ON operacao.evento_tanque USING gist (ll_to_earth(latitude, longitude));

CREATE TABLE IF NOT EXISTS operacao.sessao_tanque (
  id_sessao BIGSERIAL PRIMARY KEY,
  placa TEXT NOT NULL REFERENCES cadastro.veiculo(placa),
  tipo  TEXT NOT NULL CHECK (tipo IN ('COLETA','DESCARGA')),
  status TEXT NOT NULL DEFAULT 'ABERTA' CHECK (status IN ('ABERTA','FECHADA','DESCARTADA')),

  -- Início
  inicio_pos_id     BIGINT NOT NULL,
  inicio_data_hora  TIMESTAMPTZ NOT NULL,
  inicio_lat        DOUBLE PRECISION NOT NULL,
  inicio_lon        DOUBLE PRECISION NOT NULL,
  inicio_nivel      NUMERIC,

  -- Fim
  fim_pos_id        BIGINT,
  fim_data_hora     TIMESTAMPTZ,
  fim_lat           DOUBLE PRECISION,
  fim_lon           DOUBLE PRECISION,
  fim_nivel         NUMERIC,

  capacidade_litros_snapshot NUMERIC,
  volume_estimado_l          NUMERIC,
  raio_m_used                INTEGER,
  cooldown_min_used          INTEGER,

  pontos_validos             INTEGER NOT NULL DEFAULT 0,  -- NOVO

  atualizado_em TIMESTAMPTZ NOT NULL DEFAULT now(),
  criado_em     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_sessao_aberta_placa
  ON operacao.sessao_tanque(placa)
  WHERE status = 'ABERTA';