CREATE OR REPLACE FUNCTION operacao.registrar_evento_tanque_if_new(
    p_placa          text,
    p_tipo           text,
    p_data_hora      timestamptz,
    p_lat            double precision,
    p_lon            double precision,
    p_variacao       numeric,
    p_nivel_ant      numeric,
    p_nivel_atu      numeric,
    p_origem_posicao bigint,
    p_raio_m         integer DEFAULT 50,
    p_cooldown_min   integer DEFAULT 30
)
RETURNS boolean
LANGUAGE plpgsql
AS $$
DECLARE
    ja_existe boolean;
BEGIN
    IF p_lat IS NULL OR p_lon IS NULL THEN
        RETURN FALSE;
    END IF;

    SELECT EXISTS (
        SELECT 1
          FROM operacao.evento_tanque e
         WHERE e.placa = p_placa
           AND e.tipo  = p_tipo
           AND (p_cooldown_min IS NULL OR e.data_hora >= p_data_hora - (p_cooldown_min || ' minutes')::interval)
           AND earth_box(ll_to_earth(e.latitude, e.longitude), p_raio_m) @> ll_to_earth(p_lat, p_lon)
           AND earth_distance(ll_to_earth(e.latitude, e.longitude), ll_to_earth(p_lat, p_lon)) <= p_raio_m
         LIMIT 1
    ) INTO ja_existe;

    IF ja_existe THEN
        RETURN FALSE;
    END IF;

    INSERT INTO operacao.evento_tanque
      (placa, tipo, data_hora, latitude, longitude, variacao_percent, nivel_anterior, nivel_atual, origem_posicao)
    VALUES
      (p_placa, p_tipo, p_data_hora, p_lat, p_lon, p_variacao, p_nivel_ant, p_nivel_atu, p_origem_posicao)
    ON CONFLICT (origem_posicao) DO NOTHING;

    RETURN FOUND;
END;
$$;

CREATE OR REPLACE FUNCTION operacao._calc_volume(cap_l NUMERIC, tipo TEXT, ini NUMERIC, fim NUMERIC)
RETURNS NUMERIC
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

CREATE OR REPLACE FUNCTION operacao.registrar_evento_tanque_if_new(
    p_placa          text,
    p_tipo           text,
    p_data_hora      timestamptz,
    p_lat            double precision,
    p_lon            double precision,
    p_variacao       numeric,
    p_nivel_ant      numeric,
    p_nivel_atu      numeric,
    p_origem_posicao bigint,
    p_raio_m         integer DEFAULT 50,
    p_cooldown_min   integer DEFAULT 30
)
RETURNS boolean
LANGUAGE plpgsql
AS $$
DECLARE
    ja_existe boolean;
    cap_l     numeric;
    s_id      bigint;
    s_lat     double precision;
    s_lon     double precision;
    s_fim     timestamptz;
BEGIN
    IF p_lat IS NULL OR p_lon IS NULL THEN
        RETURN FALSE;
    END IF;

    SELECT EXISTS (
        SELECT 1
          FROM operacao.evento_tanque e
         WHERE e.placa = p_placa
           AND e.tipo  = p_tipo
           AND (p_cooldown_min IS NULL OR e.data_hora >= p_data_hora - (p_cooldown_min || ' minutes')::interval)
           AND earth_box(ll_to_earth(e.latitude, e.longitude), p_raio_m) @> ll_to_earth(p_lat, p_lon)
           AND earth_distance(ll_to_earth(e.latitude, e.longitude), ll_to_earth(p_lat, p_lon)) <= p_raio_m
         LIMIT 1
    ) INTO ja_existe;

    IF NOT ja_existe THEN
        INSERT INTO operacao.evento_tanque
          (placa, tipo, data_hora, latitude, longitude, variacao_percent, nivel_anterior, nivel_atual, origem_posicao)
        VALUES
          (p_placa, p_tipo, p_data_hora, p_lat, p_lon, p_variacao, p_nivel_ant, p_nivel_atu, p_origem_posicao)
        ON CONFLICT (origem_posicao) DO NOTHING;
    END IF;

    SELECT capacidade_tanque_litros INTO cap_l
      FROM cadastro.veiculo WHERE placa = p_placa;

    SELECT id_sessao, inicio_lat, inicio_lon, fim_data_hora
      INTO s_id, s_lat, s_lon, s_fim
      FROM operacao.sessao_tanque
     WHERE placa = p_placa AND tipo = p_tipo AND status = 'ABERTA'
     ORDER BY inicio_data_hora DESC
     LIMIT 1;

    IF FOUND THEN
        IF earth_distance(ll_to_earth(s_lat, s_lon), ll_to_earth(p_lat, p_lon)) <= p_raio_m
           AND (s_fim IS NULL OR s_fim >= p_data_hora - (p_cooldown_min || ' minutes')::interval)
        THEN
            UPDATE operacao.sessao_tanque
               SET fim_pos_id        = p_origem_posicao,
                   fim_data_hora     = p_data_hora,
                   fim_lat           = p_lat,
                   fim_lon           = p_lon,
                   fim_nivel         = p_nivel_atu,
                   volume_estimado_l = operacao._calc_volume(cap_l, p_tipo, inicio_nivel, p_nivel_atu),
                   raio_m_used       = COALESCE(raio_m_used, p_raio_m),
                   cooldown_min_used = COALESCE(cooldown_min_used, p_cooldown_min),
                   atualizado_em     = now()
             WHERE id_sessao = s_id;
        ELSE
            UPDATE operacao.sessao_tanque
               SET status = 'FECHADA', atualizado_em = now()
             WHERE id_sessao = s_id;

            INSERT INTO operacao.sessao_tanque(
                placa, tipo, status,
                inicio_pos_id, inicio_data_hora, inicio_lat, inicio_lon, inicio_nivel,
                fim_pos_id, fim_data_hora, fim_lat, fim_lon, fim_nivel,
                capacidade_litros_snapshot, volume_estimado_l, raio_m_used, cooldown_min_used
            )
            VALUES(
                p_placa, p_tipo, 'ABERTA',
                p_origem_posicao, p_data_hora, p_lat, p_lon, p_nivel_ant,
                p_origem_posicao, p_data_hora, p_lat, p_lon, p_nivel_atu,
                cap_l, operacao._calc_volume(cap_l, p_tipo, p_nivel_ant, p_nivel_atu), p_raio_m, p_cooldown_min
            );
        END IF;
    ELSE
        INSERT INTO operacao.sessao_tanque(
            placa, tipo, status,
            inicio_pos_id, inicio_data_hora, inicio_lat, inicio_lon, inicio_nivel,
            fim_pos_id, fim_data_hora, fim_lat, fim_lon, fim_nivel,
            capacidade_litros_snapshot, volume_estimado_l, raio_m_used, cooldown_min_used
        )
        VALUES(
            p_placa, p_tipo, 'ABERTA',
            p_origem_posicao, p_data_hora, p_lat, p_lon, p_nivel_ant,
            p_origem_posicao, p_data_hora, p_lat, p_lon, p_nivel_atu,
            cap_l, operacao._calc_volume(cap_l, p_tipo, p_nivel_ant, p_nivel_atu), p_raio_m, p_cooldown_min
        );
    END IF;

    RETURN TRUE;
END;
$$;

CREATE OR REPLACE FUNCTION operacao.touch_sessao_tanque(
    p_placa        text,
    p_data_hora    timestamptz,
    p_lat          double precision,
    p_lon          double precision,
    p_nivel        numeric,
    p_origem_posicao bigint,
    p_raio_m       integer DEFAULT 50,
    p_gap_min      integer DEFAULT 30
)
RETURNS boolean
LANGUAGE plpgsql
AS $$
DECLARE
    s_id   bigint;
    s_tipo text;
    s_lat  double precision;
    s_lon  double precision;
    s_fim  timestamptz;
    cap_l  numeric;
BEGIN
    SELECT id_sessao, tipo, inicio_lat, inicio_lon, fim_data_hora, capacidade_litros_snapshot
      INTO s_id, s_tipo, s_lat, s_lon, s_fim, cap_l
      FROM operacao.sessao_tanque
     WHERE placa = p_placa AND status = 'ABERTA'
     ORDER BY inicio_data_hora DESC
     LIMIT 1;

    IF NOT FOUND THEN
        RETURN FALSE;
    END IF;

    IF earth_distance(ll_to_earth(s_lat, s_lon), ll_to_earth(p_lat, p_lon)) > p_raio_m
       OR (s_fim IS NOT NULL AND p_data_hora > s_fim + (p_gap_min || ' minutes')::interval)
    THEN
        UPDATE operacao.sessao_tanque
           SET status = 'FECHADA', atualizado_em = now()
         WHERE id_sessao = s_id;
        RETURN FALSE;
    END IF;

        UPDATE operacao.sessao_tanque
       SET fim_pos_id        = p_origem_posicao,
           fim_data_hora     = p_data_hora,
           fim_lat           = p_lat,
           fim_lon           = p_lon,
           fim_nivel         = COALESCE(p_nivel, fim_nivel),
           volume_estimado_l = operacao._calc_volume(cap_l, s_tipo, inicio_nivel, COALESCE(p_nivel, fim_nivel)),
           pontos_validos    = pontos_validos + CASE WHEN p_nivel IS NOT NULL THEN 1 ELSE 0 END,  -- NOVO
           atualizado_em     = now()
     WHERE id_sessao = s_id;

    RETURN TRUE;
END;
$$;


CREATE OR REPLACE VIEW operacao.vw_sessoes_tanque AS
SELECT
  s.*,
  EXTRACT(EPOCH FROM (s.fim_data_hora - s.inicio_data_hora))::bigint AS duracao_seg
FROM operacao.sessao_tanque s
WHERE s.status = 'FECHADA';

CREATE OR REPLACE FUNCTION operacao._fechar_sessao(p_id_sessao BIGINT)
RETURNS boolean
LANGUAGE plpgsql
AS $$
DECLARE
  s RECORD;
  variacao_pp NUMERIC;
BEGIN
  SELECT * INTO s FROM operacao.sessao_tanque WHERE id_sessao = p_id_sessao FOR UPDATE;
  IF NOT FOUND OR s.fim_pos_id IS NULL OR s.fim_data_hora IS NULL THEN
    RETURN FALSE;
  END IF;

  variacao_pp := abs(COALESCE(s.fim_nivel,0) - COALESCE(s.inicio_nivel,0));

  UPDATE operacao.sessao_tanque
     SET status='FECHADA',
         volume_estimado_l = operacao._calc_volume(
                               s.capacidade_litros_snapshot, s.tipo, s.inicio_nivel, s.fim_nivel),
         atualizado_em = now()
   WHERE id_sessao = s.id_sessao;

  INSERT INTO operacao.evento_tanque
        (placa, tipo, data_hora, latitude, longitude,
         variacao_percent, nivel_anterior, nivel_atual, origem_posicao)
  VALUES (s.placa, s.tipo, s.fim_data_hora, s.fim_lat, s.fim_lon,
          variacao_pp, s.inicio_nivel, s.fim_nivel, s.fim_pos_id)
  ON CONFLICT (origem_posicao) DO NOTHING;

  RETURN TRUE;
END;
$$;


CREATE OR REPLACE FUNCTION operacao.fechar_sessoes_stagnadas(p_gap_min integer DEFAULT 30)
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
  v_count int := 0;
  r RECORD;
BEGIN
  FOR r IN
    SELECT id_sessao
      FROM operacao.sessao_tanque
     WHERE status='ABERTA'
       AND fim_data_hora IS NOT NULL
       AND fim_data_hora < now() - (p_gap_min || ' minutes')::interval
  LOOP
    PERFORM operacao._fechar_sessao(r.id_sessao);
    v_count := v_count + 1;
  END LOOP;
  RETURN v_count;
END;
$$;

CREATE INDEX IF NOT EXISTS idx_sessao_aberta_fim
  ON operacao.sessao_tanque (fim_data_hora)
  WHERE status='ABERTA' AND fim_data_hora IS NOT NULL;


CREATE OR REPLACE FUNCTION operacao._fechar_sessao(p_id_sessao BIGINT)
RETURNS boolean
LANGUAGE plpgsql
AS $$
DECLARE
  s RECORD;
  variacao_pp NUMERIC;
  dur_sec BIGINT;
  min_dur INT := 120; 
  min_pts INT := 3;
BEGIN
  SELECT * INTO s FROM operacao.sessao_tanque WHERE id_sessao = p_id_sessao FOR UPDATE;
  IF NOT FOUND OR s.fim_pos_id IS NULL OR s.fim_data_hora IS NULL THEN
    RETURN FALSE;
  END IF;

  dur_sec := EXTRACT(EPOCH FROM (s.fim_data_hora - s.inicio_data_hora))::bigint;
  variacao_pp := abs(COALESCE(s.fim_nivel,0) - COALESCE(s.inicio_nivel,0));

  IF (dur_sec < min_dur) OR (COALESCE(s.pontos_validos,0) < min_pts) THEN
    UPDATE operacao.sessao_tanque
       SET status='DESCARTADA', atualizado_em = now()
     WHERE id_sessao = s.id_sessao;
    RETURN FALSE;
  END IF;

  UPDATE operacao.sessao_tanque
     SET status='FECHADA',
         volume_estimado_l = operacao._calc_volume(
                               s.capacidade_litros_snapshot, s.tipo, s.inicio_nivel, s.fim_nivel),
         atualizado_em = now()
   WHERE id_sessao = s.id_sessao;

  INSERT INTO operacao.evento_tanque
        (placa, tipo, data_hora, latitude, longitude,
         variacao_percent, nivel_anterior, nivel_atual, origem_posicao)
  VALUES (s.placa, s.tipo, s.fim_data_hora, s.fim_lat, s.fim_lon,
          variacao_pp, s.inicio_nivel, s.fim_nivel, s.fim_pos_id)
  ON CONFLICT (origem_posicao) DO NOTHING;

  RETURN TRUE;
END;
$$;
