INSERT INTO cadastro.veiculo (placa, empresa, descricao, capacidade_tanque_litros, ativo)
VALUES
  ('9509642','TRACKLAND','CAMINHAO TESTE1', 8000, TRUE)
ON CONFLICT (placa) DO NOTHING;

