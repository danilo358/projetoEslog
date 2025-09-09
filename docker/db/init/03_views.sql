-- DB: bi_meio_ambiente

-- Garante que os schemas existem (idempotente)
CREATE SCHEMA IF NOT EXISTS cadastro;
CREATE SCHEMA IF NOT EXISTS rastreio;

-- A VIEW devolve 1 linha por placa (última posição)
CREATE OR REPLACE VIEW rastreio.v_ultima_posicao AS
WITH ult AS (
  SELECT
    p.*,
    ROW_NUMBER() OVER (
      PARTITION BY p.placa
      ORDER BY p.data_evento DESC, p.id_position DESC
    ) AS rn
  FROM rastreio.posicao p
)
SELECT
  u.placa,
  v.empresa,
  v.descricao,
  v.capacidade_tanque_litros,
  u.id_position,
  u.id_event,
  u.ignicao,
  u.valid_gps,
  u.data_evento,
  u.data_atualizacao,
  u.latitude,
  u.longitude,
  u.geom,                            -- se PostGIS estiver ativo (recomendado)
  u.nivel_tanque_percent,

  -- Exemplos de métricas vindas do JSON de telemetria:
  NULLIF(u.telemetria->>'17','')::numeric    AS velocidade_kmh,   -- ajuste a chave se seu provedor usar outra
  NULLIF(u.telemetria->>'200','')::numeric   AS hodometro,        -- idem
  NULLIF(u.telemetria->>'304','')::numeric  AS nivel_304_raw,    -- nível (bruto) vindo do 304

  u.inputs,
  u.outputs,
  u.telemetria,
  u.raw
FROM ult u
JOIN cadastro.veiculo v ON v.placa = u.placa
WHERE u.rn = 1;

-- Alias de compatibilidade (se você já referenciou este nome em algum lugar)
CREATE OR REPLACE VIEW rastreio.vw_ultimas_posicoes_detalhe AS
SELECT * FROM rastreio.v_ultima_posicao;
