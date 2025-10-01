INSERT INTO cadastro.veiculo (placa, empresa, descricao, capacidade_tanque_litros, ativo, instalado_em)
VALUES
  ('CLU9741','Lago Azul','CLU9741 - 8000L', 8000, TRUE, '2025-09-19 17:10:03.189')
ON CONFLICT (placa) DO NOTHING;

INSERT INTO cadastro.veiculo (placa, empresa, descricao, capacidade_tanque_litros, ativo, instalado_em)
VALUES
  ('BUD4281','Higibon','BUD4281 - 12000L', 12000, TRUE, '2025-09-19 18:10:00.000')
ON CONFLICT (placa) DO NOTHING;

INSERT INTO operacao.area_descarte
  (nome, lat1,lon1, lat2,lon2, lat3,lon3, lat4,lon4)
VALUES
  ('ETE',
   -21.126219, -56.465478,
   -21.126419, -56.463891,
   -21.124792, -56.463671,
   -21.124632, -56.465406
  )
ON CONFLICT DO NOTHING;