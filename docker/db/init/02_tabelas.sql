CREATE TABLE IF NOT EXISTS cadastro.veiculo (
  placa TEXT PRIMARY KEY,
  empresa TEXT,
  descricao TEXT,
  capacidade_tanque_litros NUMERIC,
  ativo BOOLEAN NOT NULL DEFAULT TRUE,
  instalado_em TIMESTAMPTZ,
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
  velocidade_kmh numeric(10,2),
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

-- Índices úteis
CREATE INDEX IF NOT EXISTS evento_tanque_placa_idx ON operacao.evento_tanque(placa);
CREATE INDEX IF NOT EXISTS evento_tanque_status_idx ON operacao.evento_tanque(status);
CREATE INDEX IF NOT EXISTS evento_tanque_inicio_idx ON operacao.evento_tanque(inicio_em);

CREATE TABLE IF NOT EXISTS operacao.sessao_tanque (
  id_sessao        BIGSERIAL PRIMARY KEY,
  placa            VARCHAR(64) NOT NULL,
  tipo             VARCHAR(16) NOT NULL CHECK (tipo IN ('COLETA','DESCARGA','DESCARTE_CORRETO','DESCARTE_INDEVIDO')),
  inicio_em        TIMESTAMPTZ NOT NULL,
  fim_em           TIMESTAMPTZ NULL,
  nivel_inicio_pct NUMERIC(6,3) NOT NULL,
  nivel_fim_pct    NUMERIC(6,3) NULL,
  lat_inicio       NUMERIC(10,6) NULL,
  lon_inicio       NUMERIC(10,6) NULL,
  lat_fim          NUMERIC(10,6) NULL,
  lon_fim          NUMERIC(10,6) NULL,
  origem           TEXT NOT NULL DEFAULT 'pp_v1',
  criado_em        TIMESTAMPTZ NOT NULL DEFAULT now(),
  atualizado_em    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 1 sessão aberta por placa+tipo (fim_em IS NULL)
CREATE UNIQUE INDEX IF NOT EXISTS sessao_tanque_open_unique
ON operacao.sessao_tanque (placa, tipo)
WHERE fim_em IS NULL;

-- 02_areas_descarte.sql
CREATE SCHEMA IF NOT EXISTS operacao;

CREATE TABLE IF NOT EXISTS operacao.area_descarte (
  id        BIGSERIAL PRIMARY KEY,
  nome      TEXT NOT NULL,

  lat1 DOUBLE PRECISION NOT NULL, lon1 DOUBLE PRECISION NOT NULL,
  lat2 DOUBLE PRECISION NOT NULL, lon2 DOUBLE PRECISION NOT NULL,
  lat3 DOUBLE PRECISION NOT NULL, lon3 DOUBLE PRECISION NOT NULL,
  lat4 DOUBLE PRECISION NOT NULL, lon4 DOUBLE PRECISION NOT NULL,

  geom geometry(Polygon, 4326)
    GENERATED ALWAYS AS (
      ST_MakePolygon(
        ST_MakeLine(ARRAY[
          ST_SetSRID(ST_MakePoint(lon1, lat1), 4326),
          ST_SetSRID(ST_MakePoint(lon2, lat2), 4326),
          ST_SetSRID(ST_MakePoint(lon3, lat3), 4326),
          ST_SetSRID(ST_MakePoint(lon4, lat4), 4326),
          ST_SetSRID(ST_MakePoint(lon1, lat1), 4326)  -- fecha o anel
        ])
      )
    ) STORED
);

CREATE INDEX IF NOT EXISTS idx_area_descarte_gix
  ON operacao.area_descarte
  USING GIST (geom);

CREATE INDEX IF NOT EXISTS sessao_tanque_placa_idx ON operacao.sessao_tanque (placa);
CREATE INDEX IF NOT EXISTS sessao_tanque_inicio_idx ON operacao.sessao_tanque (inicio_em);
CREATE INDEX IF NOT EXISTS sessao_tanque_fim_idx    ON operacao.sessao_tanque (fim_em);