===============================================================================

Visão geral

===============================================================================

- Banco: PostgreSQL 16 + extensões postgis, cube e earthdistance.
- ETL: Python 3.12 dentro de container, consulta a API (SystemSat) e grava:
  - Histórico completo em rastreio.posicao.
  - Última posição por veículo via view rastreio.v_ultima_posicao.
  - Eventos de coleta/descarga deduplicados em operacao.evento_tanque.
  - Sessões de coleta/descarga (início/fim/volume/duração) em operacao.sessao_tanque,
    expostas ao BI por operacao.vw_sessoes_tanque.
- pgAdmin: UI para administração do banco.
- Qlik: lê cadastro.veiculo, rastreio.v_ultima_posicao, operacao.evento_tanque e
  (opcional) operacao.vw_sessoes_tanque.

===============================================================================

Arquitetura dos containers

===============================================================================

- pg-bi-meio-ambiente – Postgres + PostGIS (porta host 5433 → container 5432).
- pgadmin-bi-meio-ambiente – pgAdmin (porta host 8081 → container 80).
- etl-bi-meio-ambiente – Processo cíclico que autentica, baixa posições e grava no banco.

===============================================================================

Estrutura de schemas e tabelas

===============================================================================

Criadas pelos scripts em db/init na primeira subida do banco:

- cadastro.veiculo – cadastro de placas/empresa/capacidade.
- rastreio.posicao – histórico completo de posições da API.
- operacao.evento_tanque – eventos pontuais (COLETA/DESCARGA) deduplicados.
- operacao.sessao_tanque – sessões com início/fim, volume estimado e duração.

Views:
- rastreio.v_ultima_posicao – 1 linha por placa, com a última posição (+ campos úteis da telemetria).
- rastreio.vw_ultimas_posicoes_detalhe – alias compatível para a mesma view.
- operacao.vw_sessoes_tanque – somente sessões fechadas, com duracao_seg.

===============================================================================

Funções PL/pgSQL

===============================================================================

- operacao.registrar_evento_tanque_if_new(...)
  Deduplica e grava evento pontual e abre/estende uma sessão ativa.

- operacao.touch_sessao_tanque(...)
  “Toca” a sessão aberta a cada posição nova (mesmo sem variação), atualizando fim/nivel/volume.

- operacao.fechar_sessoes_stagnadas(p_gap_min)
  Fecha sessões paradas (sem atualizações há p_gap_min minutos) e grava o evento final.

- operacao._calc_volume(cap_l, tipo, ini, fim)
  Converte variação % em litros respeitando o sentido (coleta vs descarga).

===============================================================================

Variáveis de ambiente (arquivo docker/.env)

===============================================================================

- POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB: credenciais/DB do Postgres.
- PGADMIN_DEFAULT_EMAIL, PGADMIN_DEFAULT_PASSWORD: login do pgAdmin.
- API_BASE_URL: base da API (SystemSat).
- AUTH_LOGIN_PATH, AUTH_METHOD, AUTH_USER, AUTH_PASS, AUTH_HASH: autenticação.
- GET_LAST_POSITIONS_PATH: endpoint de posições (/Controlws/HistoryPosition/List ou /.../GetLastPositions).
- CLIENT_INTEGRATION_CODE: código de cliente (se exigido pela API).
- AUTH_HEADER_NAME, AUTH_HEADER_TEMPLATE: cabeçalho para enviar o token.
- TOLERANCIA_VARIACAO_PERCENT: limiar absoluto em pp para detectar variação (ex.: 10).
- EVENT_RADIUS_METERS: raio (m) para dedupe espacial e sessões.
- EVENT_COOLDOWN_MIN: janela de cooldown (min) para dedupe temporal e fechamento por inatividade.
- FREQUENCIA_SEGUNDOS: periodicidade do ciclo do ETL.
- API_PAGE_MAX: (opcional) limite de itens retornados pela API; o ETL fatiará a janela quando atingir esse número. Padrão: 80000.

Importante (segurança): não faça commit de .env com credenciais reais. Use o etl/.env.example como referência.

===============================================================================

Como subir o ambiente

===============================================================================

Pré-requisitos:
- Docker e Docker Compose instalados.

1) Clonar e preparar .env
    cp docker/.env docker/.env.local   # edite este arquivo ou edite docker/.env diretamente

2) (Opcional) Resetar o banco
    docker compose down -v
    rm -rf db/data

3) Subir tudo
    docker compose up -d --build

4) Acessos
- pgAdmin: http://localhost:8081
  Login = variáveis PGADMIN_*.
  Cadastre a conexão em Servers → host postgres, porta 5432, usuário POSTGRES_USER, DB POSTGRES_DB.
- Postgres: host localhost, porta 5433 (mapeada), DB POSTGRES_DB.

===============================================================================

Como o ETL funciona (passo a passo)

===============================================================================

1. Login na API e guarda o token.
2. Para cada placa ativa em cadastro.veiculo:
   - Busca no banco a última data_evento da placa.
   - Se não existir registro, usa início do mês UTC como StartDatePosition.
   - Monta a janela [dt_ini, agora]. Se a API “cortar” resultados (≥ API_PAGE_MAX),
     o ETL divide recursivamente a janela em metades e soma os resultados.
   - Filtra somente posições da placa.
3. Dedup no payload por (placa, id_position) e descarta o que já existe no banco (PK id_position).
4. Ordena por data_evento ASC e insere em rastreio.posicao.
5. Para cada posição inserida, chama operacao.touch_sessao_tanque(...) para abrir/estender
   uma sessão próxima no espaço/tempo.
6. Compara o nível de tanque com valores anteriores para detectar variação ≥ limiar e
   chama operacao.registrar_evento_tanque_if_new(...) (dedupe + sessão).
7. Ao final do ciclo, executa operacao.fechar_sessoes_stagnadas(EVENT_COOLDOWN_MIN) para
   fechar sessões inativas e gravar o evento final.
8. Repete a cada FREQUENCIA_SEGUNDOS.

Nota sobre lotes grandes (ex.: 1000 posições):
Como as posições são inseridas em ordem cronológica, as funções de sessão são invocadas
também na ordem certa, garantindo que toda a operação contínua seja agrupada em uma única
sessão com volume total e duração corretos, mesmo que o veículo tenha ficado sem sinal por um tempo.

===============================================================================

Integração com Qlik

===============================================================================

O script atual do Qlik pode continuar igual para:
- cadastro.veiculo
- rastreio.v_ultima_posicao (ou rastreio.vw_ultimas_posicoes_detalhe)
- operacao.evento_tanque

Novo (opcional): adicione também operacao.vw_sessoes_tanque para obter tempo total da
coleta/descarga, volume estimado em litros e início/fim:

    SELECT * FROM operacao.vw_sessoes_tanque;

A view já traz duracao_seg calculado. Se quiser enriquecer no Qlik (formatações, buckets de duração etc.), faça no script do próprio Qlik.

===============================================================================

Operação do dia a dia

===============================================================================

- Ver logs do ETL:
    docker logs -f etl-bi-meio-ambiente

- Rodar SQL manualmente (pós-subida):
  Use o pgAdmin ou psql para reexecutar qualquer arquivo de db/init caso tenha feito alterações.

===============================================================================

Dicas e solução de problemas

===============================================================================

- “function ll_to_earth(...) does not exist”
  Certifique-se de que cube e earthdistance são criadas antes das tabelas/índices que as usam.
  Isso já é feito em 01_schemas.sql. Se você ressubiu parcialmente, rode:
    CREATE EXTENSION IF NOT EXISTS cube;
    CREATE EXTENSION IF NOT EXISTS earthdistance;

- “fechar_sessoes_stagnadas(...) does not exist”
  Garanta que o script 04_dedup.sql foi aplicado (ele cria touch_sessao_tanque,
  _fechar_sessao e fechar_sessoes_stagnadas).
  Em ambientes já existentes, rode o conteúdo do arquivo no pgAdmin.

- Muitos dados e janela grande
  Ajuste API_PAGE_MAX conforme o comportamento do endpoint. O ETL fatia automaticamente a janela.

- Time zone
  Tudo é gravado com TIMESTAMPTZ em UTC. Ajustes de exibição devem ser feitos no BI.

===============================================================================

Política de retenção

===============================================================================

Se desejar limitar a tabela de posições por placa (ex.: manter somente os N registros mais recentes),
é possível criar uma tarefa programada (cron/pgAgent) com SQL como:

    WITH ranked AS (
      SELECT id_position,
             ROW_NUMBER() OVER (PARTITION BY placa ORDER BY data_evento DESC, id_position DESC) AS rn
      FROM rastreio.posicao
    )
    DELETE FROM rastreio.posicao p
    USING ranked r
    WHERE p.id_position = r.id_position
      AND r.rn > 8000;

Atenção: retenção é opcional e não vem habilitada por padrão.
