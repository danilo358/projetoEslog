-- 03_views.sql — compatível com operacao.sessao_tanque (inicio_em/fim_em, nivel_*_pct)

-- Garante schemas
CREATE SCHEMA IF NOT EXISTS cadastro;
CREATE SCHEMA IF NOT EXISTS rastreio;
CREATE SCHEMA IF NOT EXISTS operacao;

-- Última posição por placa (ajuste mínimo; usa dados básicos já gravados em rastreio.posicao)
CREATE OR REPLACE VIEW rastreio.v_ultima_posicao AS
WITH ult AS (
  SELECT p.*,
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
  u.geom,
  u.nivel_tanque_percent,
  u.inputs,
  u.outputs,
  u.telemetria,
  u.raw
FROM ult u
JOIN cadastro.veiculo v ON v.placa = u.placa
WHERE u.rn = 1;

CREATE OR REPLACE VIEW rastreio.vw_ultimas_posicoes_detalhe AS
SELECT * FROM rastreio.v_ultima_posicao;


-- Sessões fechadas (par início/fim) — adaptado para a nova sessão sem "status"
CREATE OR REPLACE VIEW operacao.vw_sessoes_tanque_par_v2 AS
SELECT
  s.id_sessao,
  s.placa::text                               AS placa,
  s.tipo::text                                 AS tipo,               -- <- estável
  s.inicio_em                                  AS inicio_data_hora,
  s.fim_em                                     AS fim_data_hora,
  s.nivel_inicio_pct                           AS nivel_inicio_percent,
  s.nivel_fim_pct                              AS nivel_fim_percent,
  s.lat_inicio,
  s.lon_inicio,
  s.lat_fim,
  s.lon_fim,
  v.capacidade_tanque_litros                   AS capacidade_litros,
  v.capacidade_tanque_litros * s.nivel_inicio_pct / 100.0            AS litros_inicio,
  v.capacidade_tanque_litros * s.nivel_fim_pct    / 100.0            AS litros_fim,
  ABS(v.capacidade_tanque_litros * (s.nivel_fim_pct - s.nivel_inicio_pct) / 100.0) AS volume_estimado_l
FROM operacao.sessao_tanque s
JOIN cadastro.veiculo v USING (placa)
WHERE s.fim_em IS NOT NULL;



-- Painel unificado: início e fim das sessões + última posição por placa
CREATE OR REPLACE VIEW operacao.vw_painel_tanque AS
/* Início das sessões fechadas */
SELECT
  p_ini.id_position            AS id_posicao,
  s.placa,
  v.empresa,
  s.tipo::text                 AS tipo,
  'Início'::text               AS ponto,
  s.inicio_em                  AS horario,
  (v.capacidade_tanque_litros * s.nivel_inicio_pct / 100.0) AS volume_estimado_l
FROM operacao.sessao_tanque s
JOIN cadastro.veiculo v USING (placa)
LEFT JOIN LATERAL (
  SELECT id_position
    FROM rastreio.posicao
   WHERE placa = s.placa
     AND data_evento <= s.inicio_em
   ORDER BY data_evento DESC, id_position DESC
   LIMIT 1
) p_ini ON TRUE
WHERE s.fim_em IS NOT NULL

UNION ALL

/* Fim das sessões fechadas */
SELECT
  p_fim.id_position           AS id_posicao,
  s.placa,
  v.empresa,
  s.tipo::text                AS tipo,
  'Fim'::text                 AS ponto,
  s.fim_em                    AS horario,
  (v.capacidade_tanque_litros * s.nivel_fim_pct / 100.0) AS volume_estimado_l
FROM operacao.sessao_tanque s
JOIN cadastro.veiculo v USING (placa)
LEFT JOIN LATERAL (
  SELECT id_position
    FROM rastreio.posicao
   WHERE placa = s.placa
     AND data_evento <= s.fim_em
   ORDER BY data_evento DESC, id_position DESC
   LIMIT 1
) p_fim ON TRUE
WHERE s.fim_em IS NOT NULL

UNION ALL

/* Última posição por placa */
SELECT
  u.id_position                AS id_posicao,
  u.placa,
  u.empresa,
  NULL::text                   AS tipo,
  'Agora'::text                AS ponto,
  u.data_evento                AS horario,
  CASE
    WHEN u.capacidade_tanque_litros IS NOT NULL
     AND u.nivel_tanque_percent IS NOT NULL
      THEN u.capacidade_tanque_litros * (u.nivel_tanque_percent/100.0)
    ELSE NULL
  END AS volume_estimado_l
FROM rastreio.v_ultima_posicao u;

CREATE OR REPLACE VIEW operacao.vw_qlik_snapshot AS
WITH u AS (  -- última posição (Agora)
  SELECT placa, data_evento, latitude, longitude, nivel_tanque_percent
  FROM rastreio.v_ultima_posicao
),
last_closed AS (  -- última sessão fechada por placa
  SELECT DISTINCT ON (placa)
         id_sessao, placa, tipo,
         inicio_em, fim_em,
         nivel_inicio_pct, nivel_fim_pct,
         lat_inicio, lon_inicio, lat_fim, lon_fim
  FROM operacao.sessao_tanque
  WHERE fim_em IS NOT NULL
  ORDER BY placa, fim_em DESC
),
open_s AS (  -- sessão aberta mais recente (se houver)
  SELECT DISTINCT ON (placa)
         id_sessao, placa, tipo, inicio_em, nivel_inicio_pct, lat_inicio, lon_inicio
  FROM operacao.sessao_tanque
  WHERE fim_em IS NULL
  ORDER BY placa, inicio_em DESC
)
SELECT
  v.placa,
  v.empresa,
  v.descricao,
  v.capacidade_tanque_litros,

  -- Agora (última posição)
  u.data_evento                                AS agora_horario,
  CAST(u.data_evento AS date)                  AS agora_data,
  u.latitude                                   AS agora_lat,
  u.longitude                                  AS agora_lon,
  u.nivel_tanque_percent                       AS agora_nivel_percent,
  (v.capacidade_tanque_litros * u.nivel_tanque_percent/100.0) AS agora_tanque_l,

  -- Última sessão FECHADA
  lc.id_sessao                                 AS sessao_id,
  lc.tipo                                      AS sessao_tipo,
  lc.inicio_em                                 AS sessao_inicio,
  lc.fim_em                                    AS sessao_fim,
  lc.nivel_inicio_pct                          AS sessao_nivel_inicio_pct,
  lc.nivel_fim_pct                             AS sessao_nivel_fim_pct,
  (v.capacidade_tanque_litros * lc.nivel_inicio_pct/100.0)    AS litros_inicio,
  (v.capacidade_tanque_litros * lc.nivel_fim_pct/100.0)       AS litros_fim,
  ABS(v.capacidade_tanque_litros*(lc.nivel_fim_pct-lc.nivel_inicio_pct)/100.0) AS volume_estimado_l,
  lc.lat_inicio                                AS sessao_lat_inicio,
  lc.lon_inicio                                AS sessao_lon_inicio,
  lc.lat_fim                                   AS sessao_lat_fim,
  lc.lon_fim                                   AS sessao_lon_fim,

  -- Sessão ABERTA (se houver)
  os.id_sessao                                 AS sessao_aberta_id,
  os.tipo                                      AS sessao_aberta_tipo,
  os.inicio_em                                 AS sessao_aberta_inicio,
  os.nivel_inicio_pct                          AS sessao_aberta_nivel_inicio_pct,
  os.lat_inicio                                AS sessao_aberta_lat_inicio,
  os.lon_inicio                                AS sessao_aberta_lon_inicio

FROM cadastro.veiculo v
LEFT JOIN u          ON u.placa = v.placa
LEFT JOIN last_closed lc ON lc.placa = v.placa
LEFT JOIN open_s     os ON os.placa = v.placa;

CREATE OR REPLACE VIEW operacao.vw_qlik_timeline AS
WITH base AS (
  -- Sessões: gera duas linhas (Início/Fim)
  SELECT
    s.id_sessao::bigint         AS id_posicao,       -- id lógico pro gráfico
    s.placa,
    v.empresa,
    s.tipo,
    'Início'                    AS ponto,
    s.inicio_em                 AS horario,
    NULL::numeric               AS volume_estimado_l
  FROM operacao.sessao_tanque s
  JOIN cadastro.veiculo v USING (placa)
  WHERE s.inicio_em IS NOT NULL

  UNION ALL

  SELECT
    (s.id_sessao::bigint + 1000000000) AS id_posicao, -- id distinto do "Início"
    s.placa,
    v.empresa,
    s.tipo,
    'Fim'                       AS ponto,
    s.fim_em                    AS horario,
    -- volume da sessão (litros); se quiser 0 p/ DESCARGA, ajuste aqui
    ABS(v.capacidade_tanque_litros*(s.nivel_fim_pct - s.nivel_inicio_pct)/100.0) AS volume_estimado_l
  FROM operacao.sessao_tanque s
  JOIN cadastro.veiculo v USING (placa)
  WHERE s.fim_em IS NOT NULL

  UNION ALL

  -- Agora (última posição)
  SELECT
    (EXTRACT(EPOCH FROM u.data_evento))::bigint AS id_posicao,
    u.placa,
    v.empresa,
    NULL::text  AS tipo,
    'Agora'     AS ponto,
    u.data_evento AS horario,
    NULL::numeric AS volume_estimado_l
  FROM rastreio.v_ultima_posicao u
  JOIN cadastro.veiculo v USING (placa)
)
SELECT
  id_posicao,
  placa,
  empresa,
  tipo,
  ponto,
  horario,
  CAST(horario AS date) AS data_filtro,
  volume_estimado_l
FROM base;

-- Helper: ponto dentro de QUALQUER área?
CREATE OR REPLACE FUNCTION operacao._descarte_esta_em_area(p_lat DOUBLE PRECISION, p_lon DOUBLE PRECISION)
RETURNS BOOLEAN
LANGUAGE SQL
AS $$
  SELECT EXISTS (
    SELECT 1
      FROM operacao.area_descarte a
     WHERE ST_Contains(a.geom, ST_SetSRID(ST_MakePoint(p_lon, p_lat), 4326))
  );
$$;

-- Trigger: ao finalizar uma sessão de "DESCARGA",
-- trocar para DESCARTE_CORRETO ou DESCARTE_INDEVIDO.
CREATE OR REPLACE FUNCTION operacao.trg_classificar_descarte_fn()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
  -- Só atua quando a sessão acabou de ser finalizada
  IF TG_OP = 'UPDATE'
     AND NEW.fim_em IS NOT NULL
     AND (OLD.fim_em IS NULL)
     AND OLD.tipo = 'DESCARGA'
  THEN
    -- Usa lat/lon de fim; se nulos, tenta lat/lon de início
    -- (NEW.lat_fim pode ser alimentado por sessao_touch no ETL)
    IF COALESCE(NEW.lat_fim, NEW.lat_inicio) IS NOT NULL
       AND COALESCE(NEW.lon_fim, NEW.lon_inicio) IS NOT NULL
       AND operacao._descarte_esta_em_area(COALESCE(NEW.lat_fim, NEW.lat_inicio),
                                           COALESCE(NEW.lon_fim, NEW.lon_inicio))
    THEN
      NEW.tipo := 'DESCARTE_CORRETO';
    ELSE
      NEW.tipo := 'DESCARTE_INDEVIDO';
    END IF;
  END IF;

  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_classificar_descarte
ON operacao.sessao_tanque;

CREATE TRIGGER trg_classificar_descarte
AFTER UPDATE OF fim_em ON operacao.sessao_tanque
FOR EACH ROW
EXECUTE FUNCTION operacao.trg_classificar_descarte_fn();