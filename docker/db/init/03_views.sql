CREATE SCHEMA IF NOT EXISTS cadastro;
CREATE SCHEMA IF NOT EXISTS rastreio;

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
  u.geom,                         
  u.nivel_tanque_percent,

  NULLIF(u.telemetria->>'17','')::numeric    AS velocidade_kmh,
  NULLIF(u.telemetria->>'200','')::numeric   AS hodometro,
  NULLIF(u.telemetria->>'304','')::numeric  AS nivel_304_raw,

  u.inputs,
  u.outputs,
  u.telemetria,
  u.raw
FROM ult u
JOIN cadastro.veiculo v ON v.placa = u.placa
WHERE u.rn = 1;

CREATE OR REPLACE VIEW rastreio.vw_ultimas_posicoes_detalhe AS
SELECT * FROM rastreio.v_ultima_posicao;

CREATE OR REPLACE VIEW operacao.vw_sessoes_tanque_par AS
SELECT
  s.id_sessao,
  s.placa,
  s.tipo,
  s.status,
  s.inicio_data_hora,
  s.fim_data_hora,

  LEAST(s.inicio_data_hora, s.fim_data_hora)    AS hora_inicio_ord,
  GREATEST(s.inicio_data_hora, s.fim_data_hora) AS hora_fim_ord,

  -- Texto pronto para usar como dimensão
  to_char(LEAST(s.inicio_data_hora, s.fim_data_hora), 'DD/MM/YYYY HH24:MI')
  || ' → ' ||
  to_char(GREATEST(s.inicio_data_hora, s.fim_data_hora), 'DD/MM/YYYY HH24:MI')
  AS par_horario_ord,

  -- Medidas: nível (%) e litros nos dois pontos
  s.inicio_nivel                                AS nivel_inicio_percent,
  s.fim_nivel                                   AS nivel_fim_percent,
  s.capacidade_litros_snapshot                  AS capacidade_litros,
  (s.capacidade_litros_snapshot * s.inicio_nivel / 100.0) AS litros_inicio,
  (s.capacidade_litros_snapshot * s.fim_nivel    / 100.0) AS litros_fim,

  -- Já existe no modelo, mas mantemos por conveniência
  s.volume_estimado_l
FROM operacao.sessao_tanque s
WHERE s.status = 'FECHADA';

CREATE OR REPLACE VIEW operacao.vw_painel_tanque AS
/* Início das sessões (FECHADAS) */
SELECT
  s.inicio_pos_id              AS id_posicao,
  s.placa,
  v.empresa,
  s.tipo::text                 AS tipo,
  'Início'::text               AS ponto,
  s.inicio_data_hora           AS horario,
  s.volume_estimado_l          AS volume_estimado_l
FROM operacao.sessao_tanque s
JOIN cadastro.veiculo v USING (placa)
WHERE s.status = 'FECHADA'

UNION ALL
/* Fim das sessões (FECHADAS) */
SELECT
  s.fim_pos_id                 AS id_posicao,
  s.placa,
  v.empresa,
  s.tipo::text                 AS tipo,
  'Fim'::text                  AS ponto,
  s.fim_data_hora              AS horario,
  s.volume_estimado_l          AS volume_estimado_l
FROM operacao.sessao_tanque s
JOIN cadastro.veiculo v USING (placa)
WHERE s.status = 'FECHADA'
  AND s.fim_pos_id IS NOT NULL

UNION ALL
/* Última posição (“Agora”) — 1 linha por placa */
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
  END                          AS volume_estimado_l
FROM rastreio.v_ultima_posicao u;
