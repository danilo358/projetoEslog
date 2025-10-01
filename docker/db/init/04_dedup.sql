CREATE SCHEMA IF NOT EXISTS operacao;

-- ===== Helpers

CREATE OR REPLACE FUNCTION operacao._calc_volume(
  cap_l NUMERIC, tipo TEXT, ini NUMERIC, fim NUMERIC
) RETURNS NUMERIC
LANGUAGE SQL
IMMUTABLE
AS $$
  SELECT CASE
           WHEN cap_l IS NULL OR ini IS NULL OR fim IS NULL THEN NULL
           WHEN tipo = 'COLETA'   THEN cap_l * GREATEST(fim - ini, 0) / 100.0
           WHEN tipo = 'DESCARGA' THEN cap_l * GREATEST(ini - fim, 0) / 100.0
           ELSE NULL
         END
$$;

-- “Touch” em sessão ABERTA (fim_em IS NULL): atualiza fim_nivel/lat/lon e timestamp
CREATE OR REPLACE FUNCTION operacao.touch_sessao_tanque(
    p_placa           text,
    p_data_hora       timestamptz,
    p_lat             double precision,
    p_lon             double precision,
    p_nivel           numeric,
    p_origem_posicao  bigint,
    p_raio_m          integer DEFAULT 250,
    p_gap_min         integer DEFAULT 60,
    p_tipo            text DEFAULT NULL
) RETURNS boolean
LANGUAGE plpgsql
AS $$
DECLARE
    s_id   bigint;
    s_lat0 double precision;
    s_lon0 double precision;
BEGIN
    IF p_tipo IS NOT NULL THEN
        SELECT id_sessao, lat_inicio, lon_inicio
          INTO s_id, s_lat0, s_lon0
          FROM operacao.sessao_tanque
         WHERE placa = p_placa AND tipo = p_tipo AND fim_em IS NULL
         ORDER BY inicio_em DESC
         LIMIT 1;
    ELSE
        SELECT id_sessao, lat_inicio, lon_inicio
          INTO s_id, s_lat0, s_lon0
          FROM operacao.sessao_tanque
         WHERE placa = p_placa AND fim_em IS NULL
         ORDER BY inicio_em DESC
         LIMIT 1;
    END IF;

    IF NOT FOUND THEN
        RETURN FALSE;
    END IF;

    IF (p_lat IS NULL OR p_lon IS NULL)
       OR earth_distance(ll_to_earth(s_lat0, s_lon0), ll_to_earth(p_lat, p_lon)) > p_raio_m
    THEN
        RETURN FALSE;
    END IF;

    UPDATE operacao.sessao_tanque
       SET nivel_fim_pct = COALESCE(p_nivel, nivel_fim_pct),
           lat_fim       = COALESCE(p_lat, lat_fim),
           lon_fim       = COALESCE(p_lon, lon_fim),
           atualizado_em = p_data_hora
     WHERE id_sessao = s_id;

    RETURN TRUE;
END;
$$;

-- Fecha sessões “paradas”: se não recebem touch há >= p_gap_min, grava fim_em=atualizado_em
CREATE OR REPLACE FUNCTION operacao.fechar_sessoes_stagnadas(
    p_gap_min integer DEFAULT 60
) RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
  v_count int := 0;
BEGIN
  UPDATE operacao.sessao_tanque
     SET fim_em = atualizado_em, atualizado_em = now()
   WHERE fim_em IS NULL
     AND atualizado_em < now() - (p_gap_min || ' minutes')::interval;

  GET DIAGNOSTICS v_count = ROW_COUNT;
  RETURN v_count;
END;
$$;

-- ===== Views (compatíveis com a estrutura nova)

DROP VIEW IF EXISTS operacao.vw_sessoes_tanque CASCADE;
CREATE OR REPLACE VIEW operacao.vw_sessoes_tanque AS
SELECT
  s.*,
  EXTRACT(EPOCH FROM (s.fim_em - s.inicio_em))::bigint AS duracao_seg
FROM operacao.sessao_tanque s
WHERE s.fim_em IS NOT NULL;

CREATE OR REPLACE VIEW operacao.vw_areas_descarte_qlik AS
SELECT
  id,
  nome,
  ST_AsText(geom) AS wkt   -- POLYGON((lon lat, ...))
FROM operacao.area_descarte;