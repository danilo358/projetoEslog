import os
import time
import json
import logging
import requests
import yaml
import psycopg2
import unicodedata
from psycopg2.extras import execute_values
from urllib.parse import urlencode
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from dotenv import load_dotenv
import math
from collections import deque
from json.decoder import JSONDecodeError

load_dotenv()

# ====================== Configs de ambiente ======================
API_BASE_URL = os.getenv("API_BASE_URL")
CLIENT_INTEGRATION_CODE = os.getenv("CLIENT_INTEGRATION_CODE")
AUTH_LOGIN_PATH = os.getenv("AUTH_LOGIN_PATH")
AUTH_USER = os.getenv("AUTH_USER")
RADIUS_M = int(os.getenv("EVENT_RADIUS_METERS", 50))
COOLDOWN_MIN = os.getenv("EVENT_COOLDOWN_MIN")
COOLDOWN_MIN = int(COOLDOWN_MIN) if (COOLDOWN_MIN not in (None, "")) else None
AUTH_PASS = os.getenv("AUTH_PASS")
AUTH_HASH = os.getenv("AUTH_HASH")
GET_LAST_POSITIONS_PATH = os.getenv("GET_LAST_POSITIONS_PATH")

DATABASE_URL = os.getenv("DATABASE_URL")
DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "bi_meio_ambiente")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
API_PAGE_MAX = int(os.getenv("API_PAGE_MAX", "1000"))

TOLERANCIA = Decimal(os.getenv("TOLERANCIA_VARIACAO_PERCENT", "10"))
FREQUENCIA = int(os.getenv("FREQUENCIA_SEGUNDOS", "300"))

OP_IDLE_SPEED_KMH = float(os.getenv("OP_IDLE_SPEED_KMH", "2"))
OP_STATIONARY_MIN = int(os.getenv("OP_STATIONARY_MIN", "2"))
OP_MIN_SAMPLES    = int(os.getenv("OP_MIN_SAMPLES", "3"))
OP_MIN_DURATION_SEC = int(os.getenv("OP_MIN_DURATION_SEC", "120"))
OP_IDLE_RADIUS_M  = float(os.getenv("OP_IDLE_RADIUS_M", "30"))

DEBUG_HTTP = os.getenv("DEBUG_HTTP", "0") == "1"

# ====================== Logs ======================
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# ====================== Config YAML (mantido) ======================
with open(os.path.join(os.path.dirname(__file__), "config.yml"), "r", encoding="utf-8") as f:
    CONFIG = yaml.safe_load(f)

# ====================== Utilitários ======================
import os
import psycopg2

def obter_conexao():
    url = os.getenv("DATABASE_URL")
    if url:
        return psycopg2.connect(dsn=url)  # a URL já inclui sslmode

    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT", 5432),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        sslmode=os.getenv("DB_SSLMODE", "require")
    )


def _log_http_debug(resp, label="HTTP"):
    if not DEBUG_HTTP:
        return
    ctype = (resp.headers.get("Content-Type") or "").lower()
    body_preview = (resp.text or "")[:300].replace("\n", "\\n")
    try:
        req_body_len = len(resp.request.body) if resp.request and resp.request.body else 0
    except Exception:
        req_body_len = -1
    logging.info(
        f"{label} {resp.request.method} {resp.url} -> "
        f"status={resp.status_code} ctype={ctype} "
        f"req_len={req_body_len} resp_len={len(resp.content)} body^300={body_preview!r}"
    )

def _inicio_do_dia_utc() -> datetime:
    now = datetime.now(timezone.utc)
    return datetime(now.year, now.month, 8, 0, 0, 0, tzinfo=timezone.utc)

def _midpoint(a: datetime, b: datetime) -> datetime:
    return a + (b - a) / 2

import re
HTML_RE = re.compile(r"<(!DOCTYPE|html)", re.IGNORECASE)

def api_list_positions(placa: str, dt_ini: datetime, dt_fim: datetime, headers: dict, url: str) -> list[dict]:
    """
    Busca histórico na janela [dt_ini, dt_fim].
    Trata auth expirada (401/403), rate limit/5xx (retry/backoff),
    204 (sem corpo), content-type não-JSON e JSON inválido.
    Divide janela se bater API_PAGE_MAX.
    """
    body = {
        "TrackedUnitType": 1,
        "TrackedUnitIntegrationCode": placa,
        "StartDatePosition": _to_iso_z(dt_ini),
        "EndDatePosition": _to_iso_z(dt_fim),
    }
    if CLIENT_INTEGRATION_CODE:
        body["ClientIntegrationCode"] = str(CLIENT_INTEGRATION_CODE)

    # até 3 tentativas: reloga 1x em 401/403; backoff em 429/5xx
    for tentativa in range(3):
        resp = requests.post(url, json=body, headers=headers, timeout=120)
        _log_http_debug(resp, label="SSX HistoryPosition")  # <-- DEBUG ANTES DO PARSE
        # _dump_http(resp)  # se quiser ver dump completo

        # 401/403: token pode ter expirado -> reloga 1x
        if resp.status_code in (401, 403) and tentativa == 0:
            logging.warning("Auth possivelmente expirada; tentando relogar...")
            try:
                novo = login()
                header_name = os.getenv("AUTH_HEADER_NAME", "Authorization")
                header_tpl  = os.getenv("AUTH_HEADER_TEMPLATE", "Bearer {token}")
                headers = {header_name: header_tpl.format(token=novo), "Accept": "application/json"}
                continue  # refaz a tentativa com novo header
            except Exception as e:
                logging.exception(f"Falha ao relogar: {e}")
                resp.raise_for_status()

        # 204: sem conteúdo
        if resp.status_code == 204:
            return []

        # 429/5xx: retry simples com backoff
        if resp.status_code in (429, 500, 502, 503, 504):
            time.sleep(1 + tentativa)  # 1s, 2s...
            continue

        # fora de 2xx: logamos e levantamos
        if not (200 <= resp.status_code < 300):
            logging.error("API %s: %r", resp.status_code, (resp.text or "")[:300])
            resp.raise_for_status()

        # até aqui é 2xx; verifique conteúdo
        ctype = (resp.headers.get("Content-Type") or "").lower()
        txt = resp.text.strip() if resp.text is not None else ""

        if not txt:
            # 200 com corpo vazio -> trate como sem dados
            logging.info("Resposta 2xx porém sem corpo; retornando lista vazia.")
            return []

        if "application/json" not in ctype:
            # pode ser HTML de erro mesmo com 2xx/302 de gateway
            if HTML_RE.match(txt):
                logging.error("API retornou HTML em vez de JSON. Prefixo: %r", txt[:200])
                return []
            logging.warning("Content-Type inesperado: %r. Tentando parsear JSON assim mesmo.", ctype)

        try:
            itens = resp.json() or []
        except JSONDecodeError:
            logging.error("JSON inválido. Corpo (prefixo): %r", (resp.text or "")[:300])
            if tentativa < 2:
                time.sleep(1 + tentativa)
                continue
            return []

        # filtra só a placa desejada
        filtrados = []
        for it in itens:
            tu  = str(it.get("TrackedUnit") or "").strip()
            tiu = str(it.get("TrackedUnitIntegrationCode") or "").strip()
            if placa in (tu, tiu):
                filtrados.append(it)

        # se excedeu a janela/paginação, divide
        if len(filtrados) >= API_PAGE_MAX and (dt_fim - dt_ini).total_seconds() > 1:
            meio = _midpoint(dt_ini, dt_fim)
            left  = api_list_positions(placa, dt_ini, meio, headers, url)
            right = api_list_positions(placa, meio, dt_fim, headers, url)
            return left + right

        return filtrados

    # se esgotaram as tentativas
    return []



def extrair_velocidade_kmh_do_row(row) -> float | None:
    try:
        t = row.get("telemetria")
        if isinstance(t, str):
            t = json.loads(t)
        v = t.get("17") if isinstance(t, dict) else None
        if v in (None, ""):
            return None
        return float(v)
    except Exception:
        return None

def haversine_m(lat1, lon1, lat2, lon2):
    R = 6371000.0
    phi1 = math.radians(lat1); phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1); dl = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dl/2)**2
    return 2*R*math.asin(math.sqrt(a))

def janela_parada_ok(win: deque) -> bool:
    """
    Retorna True se:
      - existem pelo menos OP_MIN_SAMPLES pontos na janela
      - a janela cobre >= OP_STATIONARY_MIN minutos
      - todas as velocidades conhecidas <= OP_IDLE_SPEED_KMH
      - deslocamento entre o 1º e o último ponto <= OP_IDLE_RADIUS_M
    """
    if len(win) < OP_MIN_SAMPLES:
        return False
    t_ini = win[0]["dt"]; t_fim = win[-1]["dt"]
    if (t_fim - t_ini).total_seconds() < OP_STATIONARY_MIN * 60:
        return False

    for p in win:
        v = p["vel"]
        if (v is not None) and (v > OP_IDLE_SPEED_KMH):
            return False

    d = haversine_m(win[0]["lat"], win[0]["lon"], win[-1]["lat"], win[-1]["lon"])
    return d <= OP_IDLE_RADIUS_M

def variacao_em_pp(win: deque) -> Decimal | None:
    n0 = None
    for p in win:
        if p["nivel"] is not None:
            n0 = Decimal(str(p["nivel"]))
            break
    n1 = None
    for p in reversed(win):
        if p["nivel"] is not None:
            n1 = Decimal(str(p["nivel"]))
            break
    if n0 is None or n1 is None:
        return None
    return n1 - n0

def _to_iso_z(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")

def _parse_dt_any(s: str) -> datetime:
    if not s:
        return datetime(1970, 1, 1, tzinfo=timezone.utc)
    s_norm = s
    if s_norm.endswith('Z'):
        s_norm = s_norm[:-1] + '+00:00'
    try:
        return datetime.fromisoformat(s_norm)
    except Exception:
        try:
            return datetime.strptime(s_norm, "%Y-%m-%d %H:%M:%S%z")
        except Exception:
            return datetime(1970, 1, 1, tzinfo=timezone.utc)

def obter_ultima_data_posicao(cur, placa: str) -> datetime | None:
    cur.execute("SELECT MAX(data_evento) FROM rastreio.posicao WHERE placa = %s;", (placa,))
    row = cur.fetchone()
    return row[0]

def _sanitize_path(p: str) -> str:
    if not p:
        return ""
    p = "".join(ch for ch in p if unicodedata.category(ch)[0] != "C")
    p = p.strip()
    if not p.startswith("/"):
        p = "/" + p
    return p

def _build_url(base: str, path: str) -> str:
    base = (base or "").rstrip("/")
    path = _sanitize_path(path or "")
    return f"{base}{path}"

# ====================== Auth ======================
def login():
    base = (API_BASE_URL or "").rstrip("/")
    path = (AUTH_LOGIN_PATH or "/Login").lstrip("/")
    url = f"{base}/{path}"

    q_user_key = os.getenv("AUTH_QUERY_USER_KEY", "Username")
    q_pass_key = os.getenv("AUTH_QUERY_PASS_KEY", "Password")
    q_hash_key = os.getenv("AUTH_QUERY_HASH_KEY", "HashAuth")
    q_hash_val = os.getenv("AUTH_HASH")

    params = {q_user_key: AUTH_USER, q_pass_key: AUTH_PASS}
    if q_hash_key and q_hash_val:
        params[q_hash_key] = q_hash_val

    method = (os.getenv("AUTH_METHOD") or "POST_PARAMS").upper()

    if method == "GET_PARAMS":
        resp = requests.get(url, params=params, headers={"Accept": "application/json"}, timeout=30)
    elif method == "POST_FORM":
        resp = requests.post(url, data=params, headers={"Accept": "application/json"}, timeout=30)
    else:  # POST_PARAMS
        resp = requests.post(url, params=params, headers={"Accept": "application/json"}, timeout=30)

    if not (200 <= resp.status_code < 300):
        raise RuntimeError(f"Login HTTP {resp.status_code}: prefixo: {resp.text[:200]!r}")

    token = None
    try:
        data = resp.json()
        if isinstance(data, dict):
            token = data.get("AccessToken") or data.get("authToken") or data.get("token")
    except Exception:
        data = None

    if not token:
        token = resp.headers.get("AccessToken") or resp.headers.get("AuthToken")

    if not token:
        txt = resp.text.strip()
        if txt.startswith("<!DOCTYPE") or txt.startswith("<html"):
            raise RuntimeError(f"Login retornou HTML. Endpoint incorreto? prefixo: {txt[:120]!r}")
        token = txt

    token = str(token).strip()
    if not token or "<" in token or "\n" in token or "\r" in token:
        raise RuntimeError(f"Token inválido: {token[:120]!r}")

    return token



# ====================== Regras de negócio ======================
def extrair_nivel_tanque(telemetria: dict, placa: str) -> Decimal | None:
    """
    SOMENTE o código 304 de ListTelemetry. Nada além disso.
    """
    if not telemetria:
        return None
    v = telemetria.get("304")
    if v is None:
        v = telemetria.get(304)
    return Decimal(str(v)) if v is not None else None

def carregar_placas_validas(cur) -> list[str]:
    cur.execute("SELECT placa FROM cadastro.veiculo WHERE ativo = TRUE;")
    return [str(r[0]).strip() for r in cur.fetchall()]

def carregar_ids_existentes(cur, ids: list[int]) -> set[int]:
    existentes = set()
    if not ids:
        return existentes
    CHUNK = 1000
    for i in range(0, len(ids), CHUNK):
        slice_ids = ids[i:i+CHUNK]
        cur.execute("SELECT id_position FROM rastreio.posicao WHERE id_position = ANY(%s);", (slice_ids,))
        existentes.update(r[0] for r in cur.fetchall())
    return existentes

def obter_ultimos_niveis_antes(cur, placa: str, data_corte: str, limite: int = 10) -> list[Decimal]:
    """
    Retorna até 'limite' níveis anteriores (não nulos) ANTES de data_corte, em ORDEM CRONOLÓGICA ASC.
    """
    cur.execute("""
        SELECT nivel_tanque_percent
          FROM rastreio.posicao
         WHERE placa = %s
           AND nivel_tanque_percent IS NOT NULL
           AND data_evento < %s
         ORDER BY data_evento DESC
         LIMIT %s;
    """, (placa, data_corte, limite))
    rows = cur.fetchall()
    return [Decimal(str(r[0])) for r in rows[::-1]]

def obter_ultima_posicao_com_nivel(cur, placa: str):
    cur.execute("""
        SELECT id_position, data_evento, nivel_tanque_percent
        FROM rastreio.posicao
        WHERE placa = %s AND nivel_tanque_percent IS NOT NULL
        ORDER BY data_evento DESC
        LIMIT 1;
    """, (placa,))
    return cur.fetchone()

def inserir_evento_tanque(cur, placa, tipo, data_hora, lat, lon, variacao_pp, nivel_ant, nivel_atu, origem_posicao):
    if lat is None or lon is None:
        return  # sem coordenada, não tenta registrar

    cur.execute("""
        SELECT operacao.registrar_evento_tanque_if_new(
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
        );
    """, (
        placa, tipo, data_hora, float(lat), float(lon),
        float(variacao_pp),
        float(nivel_ant) if nivel_ant is not None else None,
        float(nivel_atu)  if nivel_atu  is not None else None,
        origem_posicao,
        RADIUS_M,
        COOLDOWN_MIN
    ))


from psycopg2.extras import execute_values

def inserir_posicoes(cur, linhas):
    if not linhas:
        return []
    cols = ("id_position","placa","id_event","ignicao","valid_gps","data_evento",
            "data_atualizacao","latitude","longitude","inputs","outputs",
            "telemetria","nivel_tanque_percent","raw")
    tpl = "(" + ",".join([f"%({c})s" for c in cols]) + ")"
    sql = f"""
        INSERT INTO rastreio.posicao ({",".join(cols)})
        VALUES %s
        ON CONFLICT (id_position) DO NOTHING;
    """
    execute_values(cur, sql, linhas, template=tpl, page_size=1000)
    return linhas


# ====================== ETL ======================
def coletar_e_gravar():
    token = login()
    header_name = os.getenv("AUTH_HEADER_NAME", "Authorization")
    header_tpl  = os.getenv("AUTH_HEADER_TEMPLATE", "Bearer {token}")
    headers = {header_name: header_tpl.format(token=token), "Accept": "application/json"}
    url = f"{API_BASE_URL}{GET_LAST_POSITIONS_PATH}"

    with obter_conexao() as conn, conn.cursor() as cur:
        placas_validas = sorted(carregar_placas_validas(cur))
        candidatos = []
        agora = datetime.now(timezone.utc)

        for placa in placas_validas:
            try:
                dt_ultimo = obter_ultima_data_posicao(cur, placa)
                dt_ini = _inicio_do_dia_utc() if dt_ultimo is None else (
                    (dt_ultimo.astimezone(timezone.utc) if dt_ultimo.tzinfo else dt_ultimo.replace(tzinfo=timezone.utc))
                    + timedelta(milliseconds=1)
                )
                dt_fim = agora
                if dt_ini >= dt_fim:
                    dt_ini = dt_fim - timedelta(seconds=1)

                logging.info(f"[{placa}] janela {_to_iso_z(dt_ini)} -> {_to_iso_z(dt_fim)} (última no banco: {dt_ultimo})")

                # >>>>>> CHAMADA ÚNICA (sem POST redundante)
                itens = api_list_positions(placa, dt_ini, dt_fim, headers, url)
                logging.info(f"[{placa}] posições retornadas pela API (após janela/poda): {len(itens)}")

                for item in itens:
                    try:
                        idp = int(item["IdPosition"])
                    except (KeyError, TypeError, ValueError):
                        continue

                    telemetria = item.get("ListTelemetry") or {}
                    nivel = extrair_nivel_tanque(telemetria, placa)

                    candidatos.append({
                        "id_position": idp,
                        "placa": placa,
                        "id_event": item.get("IdEvent"),
                        "ignicao": item.get("Ignition"),
                        "valid_gps": item.get("ValidGPS"),
                        "data_evento": item.get("EventDate"),
                        "data_atualizacao": item.get("UpdateDate"),
                        "latitude": item.get("Latitude"),
                        "longitude": item.get("Longitude"),
                        "inputs": json.dumps(item.get("ListInputSensor") or {}),
                        "outputs": json.dumps(item.get("ListOutputActuator") or {}),
                        "telemetria": json.dumps(telemetria),
                        "nivel_tanque_percent": float(nivel) if nivel is not None else None,
                        "raw": json.dumps(item),
                    })

            except Exception as e:
                logging.exception(f"Falha ao consultar histórico da placa {placa}: {e}")

        if not candidatos:
            logging.info("Nenhuma posição retornada pelas consultas de histórico.")
            return

        # --- Dedup por (placa, id_position) ---
        mapa = {(c["placa"], c["id_position"]): c for c in candidatos}
        candidatos = list(mapa.values())

        # --- Elimina os que já existem ---
        ids_candidatos = [c["id_position"] for c in candidatos]
        existentes = carregar_ids_existentes(cur, ids_candidatos)
        linhas_novas = [c for c in candidatos if c["id_position"] not in existentes]

        if not linhas_novas:
            logging.info("Nenhuma posição nova (todas já existem).")
            return

        # --- Ordena por data_evento ASC ---
        linhas_novas.sort(key=lambda r: _parse_dt_any(r["data_evento"]))

        # --- Insere (na ordem) ---
        linhas_inseridas = inserir_posicoes(cur, linhas_novas)

        # --- Touch sessões tanque ---
        for r in linhas_inseridas:
            try:
                cur.execute("""
                    SELECT operacao.touch_sessao_tanque(
                        %s,%s,%s,%s,%s,%s,%s,%s
                    );
                """, (
                    r["placa"], r["data_evento"],
                    r["latitude"], r["longitude"],
                    r["nivel_tanque_percent"],
                    r["id_position"],
                    RADIUS_M, COOLDOWN_MIN or 30
                ))
            except Exception as e:
                logging.exception(f"touch_sessao_tanque falhou p/ {r['placa']} pos {r['id_position']}: {e}")

        logging.info(f"Posições novas inseridas: {len(linhas_inseridas)}")
        cont_por_placa = {}
        for r in linhas_inseridas:
            cont_por_placa[r["placa"]] = cont_por_placa.get(r["placa"], 0) + 1
        for p, q in cont_por_placa.items():
            logging.info(f"[{p}] novas inseridas: {q}")

        # ===== Geração de eventos de tanque =====
        eventos_criados = 0
        por_placa = {}
        for r in linhas_inseridas:
            por_placa.setdefault(r["placa"], []).append(r)

        for placa, lista in por_placa.items():
            lista.sort(key=lambda r: _parse_dt_any(r["data_evento"]))
            win = deque()

            for linha in lista:
                dt = _parse_dt_any(linha["data_evento"])
                vel = extrair_velocidade_kmh_do_row(linha)
                pt = {"dt": dt, "lat": linha["latitude"], "lon": linha["longitude"],
                      "nivel": linha["nivel_tanque_percent"], "vel": vel, "idp": linha["id_position"]}

                win.append(pt)
                while win and (dt - win[0]["dt"]).total_seconds() > OP_STATIONARY_MIN*60:
                    win.popleft()

                try:
                    cur.execute("""
                        SELECT operacao.touch_sessao_tanque(
                            %s,%s,%s,%s,%s,%s,%s,%s
                        );
                    """, (
                        linha["placa"], linha["data_evento"],
                        linha["latitude"], linha["longitude"],
                        linha["nivel_tanque_percent"],
                        linha["id_position"],
                        RADIUS_M, COOLDOWN_MIN or 30
                    ))
                except Exception as e:
                    logging.exception(f"touch_sessao_tanque falhou p/ {linha['placa']} pos {linha['id_position']}: {e}")

                if janela_parada_ok(win):
                    diff = variacao_em_pp(win)
                    if diff is not None and abs(diff) >= TOLERANCIA:
                        tipo = "COLETA" if diff > 0 else "DESCARGA"
                        # primeiro e último nível da janela
                        nivel_ant = next((Decimal(str(p["nivel"])) for p in win if p["nivel"] is not None), None)
                        nivel_atu = next((Decimal(str(p["nivel"])) for p in reversed(win) if p["nivel"] is not None), None)

                        inserir_evento_tanque(
                            cur, placa, tipo, linha["data_evento"],
                            linha["latitude"], linha["longitude"],
                            abs(diff), nivel_ant, nivel_atu, linha["id_position"]
                        )
                        eventos_criados += 1

            conn.commit()
            logging.info(f"[{placa}] commit parcial (sessões atualizadas).")

        try:
            cur.execute("SELECT operacao.fechar_sessoes_stagnadas(%s);", (COOLDOWN_MIN or 30,))
        except Exception as e:
            logging.exception(f"fechar_sessoes_stagnadas falhou: {e}")

        conn.commit()
        logging.info(f"Eventos gerados neste ciclo: {eventos_criados}")


# ====================== Loop ======================
def loop():
    while True:
        try:
            coletar_e_gravar()
        except Exception as e:
            logging.exception(f"Falha no ciclo de ETL: {e}")
        time.sleep(FREQUENCIA)

if __name__ == "__main__":
    loop()
