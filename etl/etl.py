import os, time, json, logging, requests, unicodedata
from urllib.parse import urlencode
from datetime import datetime, timezone, timedelta
from decimal import Decimal, InvalidOperation
from collections import deque
from json.decoder import JSONDecodeError
from zoneinfo import ZoneInfo
from math import radians, sin, cos, atan2, sqrt
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# ====================== bootstrap ======================
load_dotenv()

# ====================== Ambiente / .env ======================
API_BASE_URL = os.getenv("API_BASE_URL")
CLIENT_INTEGRATION_CODE = os.getenv("CLIENT_INTEGRATION_CODE")
AUTH_LOGIN_PATH = os.getenv("AUTH_LOGIN_PATH")
AUTH_USER = os.getenv("AUTH_USER")
AUTH_PASS = os.getenv("AUTH_PASS")
AUTH_HASH = os.getenv("AUTH_HASH")
GET_LAST_POSITIONS_PATH = os.getenv("GET_LAST_POSITIONS_PATH")

AUTH_HEADER_NAME = os.getenv("AUTH_HEADER_NAME", "Authorization")
AUTH_HEADER_TEMPLATE = os.getenv("AUTH_HEADER_TEMPLATE", "Bearer {token}")
AUTH_METHOD = (os.getenv("AUTH_METHOD") or "POST_PARAMS").upper()
AUTH_QUERY_USER_KEY = os.getenv("AUTH_QUERY_USER_KEY", "Username")
AUTH_QUERY_PASS_KEY = os.getenv("AUTH_QUERY_PASS_KEY", "Password")
AUTH_QUERY_HASH_KEY = os.getenv("AUTH_QUERY_HASH_KEY", "HashAuth")

DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "eslog")
DB_USER = os.getenv("DB_USER", "eslog")
DB_PASS = os.getenv("DB_PASS", "eslog123")
DB_SSLMODE = os.getenv("DB_SSLMODE", "require")
DB_CHANNEL_BINDING = os.getenv("DB_channel_binding", "require")
#DATABASE_URL = os.getenv("DATABASE_URL")


API_PAGE_MAX = int(os.getenv("API_PAGE_MAX", "10000"))
FREQUENCIA = int(os.getenv("FREQUENCIA_SEGUNDOS", "10"))
DEBUG_HTTP = os.getenv("DEBUG_HTTP", "0") == "1"
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

LOCAL_TZ_NAME = os.getenv("TZ", "America/Campo_Grande")
LOCAL_TZ = ZoneInfo(LOCAL_TZ_NAME)

FIX_UTC_OFFSET_HOURS = int(os.getenv("FIX_UTC_OFFSET_HOURS", "0"))
SAVE_AS_NAIVE_LOCAL = (os.getenv("SAVE_AS_NAIVE_LOCAL", "true").lower() == "true")

# -------- Detector por Tendência (v2) --------
TREND_WINDOW_POINTS = int(os.getenv("TREND_WINDOW_POINTS", "5"))
TREND_START_THRESHOLD_PP = Decimal(os.getenv("TREND_START_THRESHOLD_PP", "3.0"))
TREND_STOP_THRESHOLD_PP = Decimal(os.getenv("TREND_STOP_THRESHOLD_PP", "1.0"))
TREND_CONFIRMATION_WINDOWS = int(os.getenv("TREND_CONFIRMATION_WINDOWS", "2"))
MAX_INV_PCT = Decimal(os.getenv("MAX_INV_PCT", "5"))

# -------- Critérios de Validação de Sessão --------
MIN_SESSION_DURATION_SEC = int(os.getenv("MIN_SESSION_DURATION_SEC", "60"))
MIN_SESSION_DELTA_PP = Decimal(os.getenv("MIN_SESSION_DELTA_PP", "5"))
GAP_MIN = int(os.getenv("GAP_MIN", "60"))

# ---- Modo "slow trend" (config) ----
SLOW_WIN_SEC = int(os.getenv("SLOW_TREND_WINDOW_SEC","600"))
SLOW_RANGE   = float(os.getenv("SLOW_TREND_MIN_RANGE_PP","6"))
SLOW_NEGFR   = float(os.getenv("SLOW_TREND_NEG_FRAC","0.7"))
TOUCH_ONLY_WHEN_STOPPED = os.getenv("TOUCH_ONLY_WHEN_STOPPED","1") == "1"
SPEED_STOP_MAX_KMH = float(os.getenv("SPEED_STOP_MAX_KMH","10"))
# ---- Anti-spike (config) ----
SPIKE_MIN_JUMP_PP = float(os.getenv("SPIKE_MIN_JUMP_PP", "5"))
SPIKE_REV_WIN_SEC = int(os.getenv("SPIKE_REVERSAL_WINDOW_SEC", "180"))
SPIKE_TOL_BAND_PP = float(os.getenv("SPIKE_REVERSAL_TOL_BAND_PP", "2"))
MIN_DWELL_NEW_LEVEL_SEC = int(os.getenv("MIN_DWELL_NEW_LEVEL_SEC","120"))
# ---- Geofence / Parada para retomar ----
EXIT_RADIUS_M = int(os.getenv("EXIT_RADIUS_M", "250"))
RESUME_STOP_DWELL_SEC = int(os.getenv("RESUME_STOP_DWELL_SEC", "0"))

# ====================== Logs ======================
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s [%(levelname)s] %(message)s")
logging.Formatter.converter = lambda *args: datetime.now(LOCAL_TZ).timetuple()
logging.info(
    f"Detector de Tendência (v2) INICIADO com cfg: "
    f"window_pts={TREND_WINDOW_POINTS}, start_pp={TREND_START_THRESHOLD_PP}, "
    f"stop_pp={TREND_STOP_THRESHOLD_PP}, confirm_windows={TREND_CONFIRMATION_WINDOWS}, "
    f"min_sess={MIN_SESSION_DURATION_SEC}s/{MIN_SESSION_DELTA_PP}pp"
    f"window_pts={TREND_WINDOW_POINTS}, start_pp={float(TREND_START_THRESHOLD_PP):.2f}, "
    f"stop_pp={float(TREND_STOP_THRESHOLD_PP):.2f}, confirm_windows={TREND_CONFIRMATION_WINDOWS}, "
    f"min_sess={MIN_SESSION_DURATION_SEC}s/{float(MIN_SESSION_DELTA_PP):.2f}pp; "
    f"slow(win={SLOW_WIN_SEC}s,range>={SLOW_RANGE}pp,neg>={SLOW_NEGFR:.0%}); "
    f"spike(jump>={SPIKE_MIN_JUMP_PP}pp,rev<={SPIKE_REV_WIN_SEC}s,band±{SPIKE_TOL_BAND_PP}pp); "
    f"stopped={TOUCH_ONLY_WHEN_STOPPED}(≤{SPEED_STOP_MAX_KMH}km/h)"
)

# ====================== Utils gerais ======================
def _to_iso_z(dt: datetime) -> str:
    if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
    else: dt = dt.astimezone(timezone.utc)
    return dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")

def _parse_dt_any(s: str) -> datetime:
    if not s: return datetime(1970, 1, 1, tzinfo=LOCAL_TZ)
    s_norm = s[:-1] + "+00:00" if s.endswith("Z") else s
    try: return datetime.fromisoformat(s_norm)
    except (ValueError, TypeError):
        try: return datetime.strptime(s_norm, "%Y-%m-%d %H:%M:%S%z")
        except (ValueError, TypeError): return datetime(1970, 1, 1, tzinfo=LOCAL_TZ)

def _to_local_dt(s: str) -> datetime:
    dt = _parse_dt_any(s)
    if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(LOCAL_TZ)

def _sanitize_path(p: str) -> str:
    if not p: return ""
    p = "".join(ch for ch in p if unicodedata.category(ch)[0] != "C").strip()
    return f"/{p}" if not p.startswith("/") else p

def _build_url(base: str, path: str) -> str:
    base = (base or "").rstrip("/")
    path = _sanitize_path(path or "")
    return f"{base}{path}"

def _haversine_m(lat1, lon1, lat2, lon2) -> float:
    """Distância em metros entre dois pares lat/lon (WGS84)."""
    if None in (lat1, lon1, lat2, lon2):
        return float("inf")
    R = 6371000.0  # raio médio da Terra em metros
    φ1, φ2 = radians(float(lat1)), radians(float(lat2))
    Δφ, Δλ = radians(float(lat2) - float(lat1)), radians(float(lon2) - float(lon1))
    a = sin(Δφ/2)**2 + cos(φ1)*cos(φ2)*sin(Δλ/2)**2
    return 2 * R * atan2(sqrt(a), sqrt(1-a))

def _to_db_ts(s: str):
    """
    Converte string de data (API) para datetime pronto para inserir no Postgres,
    aplicando FIX_UTC_OFFSET_HOURS ANTES de salvar.
    - Se SAVE_AS_NAIVE_LOCAL=True: remove tzinfo e grava 'naive' (timestamp sem tz) já com o shift aplicado.
    - Se SAVE_AS_NAIVE_LOCAL=False: mantém tzinfo (aware) porém com o horário já shiftado.
    """
    dt = _parse_dt_any(s)
    # considere que a API vem em UTC (GMT)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)

    # aplica o deslocamento desejado (ex.: -4h)
    dt_shift = dt + timedelta(hours=FIX_UTC_OFFSET_HOURS)

    if SAVE_AS_NAIVE_LOCAL:
        # grava como "timestamp sem timezone" no banco, já corrigido
        return dt_shift.replace(tzinfo=None)
    else:
        # grava como tz-aware (psycopg2 manda com tz e o PG converte p/ UTC internamente)
        return dt_shift

def _naive_local(dt: datetime) -> datetime:
    """Normaliza qualquer datetime para 'naive' no fuso LOCAL_TZ."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt
    return dt.astimezone(LOCAL_TZ).replace(tzinfo=None)

# ====================== DB ======================
def obter_conexao():
    #if DATABASE_URL and DATABASE_URL.strip():
    #   return psycopg2.connect(dsn=DATABASE_URL)
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS, sslmode=DB_SSLMODE, channel_binding=DB_CHANNEL_BINDING
    )

def carregar_placas_validas(cur):
    cur.execute("SELECT placa FROM cadastro.veiculo WHERE ativo = TRUE;")
    return [str(r[0]).strip() for r in cur.fetchall()]

def carregar_ids_existentes(cur, ids):
    existentes = set()
    if not ids: return existentes
    CHUNK = 10000
    for i in range(0, len(ids), CHUNK):
        slice_ids = ids[i:i+CHUNK]
        cur.execute("SELECT id_position FROM rastreio.posicao WHERE id_position = ANY(%s);", (slice_ids,))
        existentes.update(r[0] for r in cur.fetchall())
    return existentes

def obter_ultima_data_posicao(cur, placa: str):
    cur.execute("SELECT MAX(data_evento) FROM rastreio.posicao WHERE placa = %s;", (placa,))
    return cur.fetchone()[0]

def obter_data_instalacao(cur, placa: str):
    cur.execute("SELECT instalado_em FROM cadastro.veiculo WHERE placa = %s;", (placa,))
    return cur.fetchone()[0]


# ====================== Regras de Sessão ======================
def sessao_abrir(cur, placa, tipo, t, nivel_ini_pct, lat_ini=None, lon_ini=None, origem='trend_v2'):
    cur.execute("SELECT id_sessao FROM operacao.sessao_tanque WHERE placa=%s AND tipo=%s AND fim_em IS NULL ORDER BY inicio_em DESC LIMIT 1", (placa, tipo))
    if cur.fetchone(): return None
    cur.execute(
        "INSERT INTO operacao.sessao_tanque (placa,tipo,inicio_em,nivel_inicio_pct,lat_inicio,lon_inicio,origem) VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING id_sessao",
        (placa, tipo, t, float(nivel_ini_pct), lat_ini, lon_ini, origem)
    )
    return cur.fetchone()[0]

def sessao_touch(cur, id_sessao, t, nivel_fim_pct, lat_fim=None, lon_fim=None):
    cur.execute(
        "UPDATE operacao.sessao_tanque SET nivel_fim_pct=%s, lat_fim=%s, lon_fim=%s, atualizado_em=%s WHERE id_sessao=%s AND fim_em IS NULL",
        (float(nivel_fim_pct), lat_fim, lon_fim, t, id_sessao)
    )

def sessao_finalizar(cur, id_sessao):
    cur.execute(
        "UPDATE operacao.sessao_tanque SET fim_em=atualizado_em, atualizado_em=now() WHERE id_sessao=%s AND fim_em IS NULL",
        (id_sessao,)
    )

def sessao_cancelar(cur, id_sessao):
    cur.execute("DELETE FROM operacao.sessao_tanque WHERE id_sessao = %s", (id_sessao,))

# etl.py

def finalizar_sessao_se_valida(cur, sess_dict):
    if not sess_dict or not sess_dict.get("last_touch_t"): return 0
    
    MIN_POINTS = 2
    
    point_count = sess_dict.get("point_count", 0)
    dur = (sess_dict["last_touch_t"] - sess_dict["t0"]).total_seconds()
    delta_pp = abs(sess_dict["last_nivel"] - sess_dict["nivel0"])
    
    if dur >= MIN_SESSION_DURATION_SEC and delta_pp >= MIN_SESSION_DELTA_PP and point_count >= MIN_POINTS:
        logging.info(f"Finalizando sessão válida {sess_dict['id']} para {sess_dict['placa']} (pontos únicos: {point_count}, duração: {dur:.0f}s, delta: {delta_pp:.2f}pp)")
        sessao_finalizar(cur, sess_dict["id"])
        return 1
    else:
        logging.warning(
            f"Cancelando sessão inválida {sess_dict['id']} para {sess_dict['placa']} "
            f"(pontos únicos: {point_count}, duração: {dur:.0f}s, delta: {delta_pp:.2f}pp) - "
            f"Critérios: min_pontos={MIN_POINTS}, min_dur={MIN_SESSION_DURATION_SEC}s, min_delta={MIN_SESSION_DELTA_PP}pp"
        )
        sessao_cancelar(cur, sess_dict["id"])
        return 0

# ====================== Detector por Tendência (v2) ======================
# Adicione esta função ANTES de detect_events_by_trend:

def detect_events_with_context(cur, placa, new_rows, lookback_minutes):
    """
    Busca contexto histórico antes de detectar eventos.
    Isso garante que eventos que cruzam ciclos de ETL sejam capturados.
    """
    if not new_rows:
        return 0
    
    # Pegar timestamp do primeiro ponto novo
    first_new_time = min(r["data_evento"] for r in new_rows)
    lookback_time = first_new_time - timedelta(minutes=lookback_minutes)
    
    # Buscar pontos históricos recentes
    cur.execute("""
        SELECT 
            data_evento,
            nivel_tanque_percent,
            latitude,
            longitude,
            velocidade_kmh
        FROM rastreio.posicao
        WHERE placa = %s
          AND data_evento BETWEEN %s AND %s
          AND nivel_tanque_percent IS NOT NULL
          AND nivel_tanque_percent > 0
        ORDER BY data_evento, id_position
    """, (placa, lookback_time, first_new_time))
    
    historical_rows = [
        {
            "data_evento": row[0],
            "nivel_tanque_percent": row[1],
            "latitude": row[2],
            "longitude": row[3],
            "velocidade_kmh": row[4]
        }
        for row in cur.fetchall()
    ]
    
    # Combinar histórico + novos pontos
    all_rows = historical_rows + new_rows
    all_rows.sort(key=lambda r: _naive_local["data_evento"])
    
    logging.info(f"[{placa}] Detectando com contexto: {len(historical_rows)} históricos + {len(new_rows)} novos")
    
    return detect_events_by_trend(cur, placa, all_rows)

# Substitua a função detect_events_by_trend completa por esta versão:

def detect_events_by_trend(cur, placa, rows):
    finalizados = 0
    state = "STABLE"
    open_sess = None

    # GEOfence/Retomar-Parado: trava detecção após sair do raio até parar
    block_until_stopped = False
    stop_since_t = None

    trend_confirmation_count = 0
    potential_sess_start = None
    slow_conf_count = 0

    MAX_STALE_TIME_MIN = int(os.getenv("MAX_STALE_TIME_MIN", "20"))
    MAX_SESSION_DURATION_MIN = int(os.getenv("MAX_SESSION_DURATION_MIN", "90"))

    # Retomar sessão se existir
    cur.execute("""
        SELECT id_sessao, tipo, inicio_em, nivel_inicio_pct, 
               lat_inicio, lon_inicio, atualizado_em, nivel_fim_pct
        FROM operacao.sessao_tanque
        WHERE placa = %s AND fim_em IS NULL
        ORDER BY inicio_em DESC
        LIMIT 1
    """, (placa,))
    existing = cur.fetchone()
    if existing:
        sid, tipo, t0, nivel0, lat0, lon0, last_updated, last_nivel = existing
        logging.info(f"[{placa}] Retomando sessão existente {sid} ({tipo}) iniciada em {t0}")
        open_sess = {
            "id": sid, "placa": placa, "tipo": tipo,
            "t0": t0,
            "nivel0": Decimal(str(nivel0)),
            "last_touch_t": last_updated,
            "last_nivel": Decimal(str(last_nivel)) if last_nivel else Decimal(str(nivel0)),
            "point_count": 1,
            "last_unique_nv": Decimal(str(last_nivel)) if last_nivel else Decimal(str(nivel0)),
            "lat0": float(lat0) if lat0 is not None else None,
            "lon0": float(lon0) if lon0 is not None else None,
        }
        state = "TRENDING_DOWN" if tipo == "DESCARGA" else "TRENDING_UP"

    def _get_point_details(r):
        try:
            nv_raw = r.get("nivel_tanque_percent")
            nv = Decimal(str(nv_raw)) if nv_raw is not None and nv_raw > 0 else None
            return {
                "t": r.get("data_evento"),
                "nv": nv,
                "lat": r.get("latitude"),
                "lon": r.get("longitude"),
                "v": r.get("velocidade_kmh"),
            }
        except (InvalidOperation, TypeError):
            return {"t": r.get("data_evento"), "nv": None}

    # 1) Filtrar pontos com nível válido (detecção independe de velocidade por enquanto)
    valid_points = []
    for r in rows:
        p = _get_point_details(r)
        if p["t"] is None or p["nv"] is None:
            continue
        valid_points.append(p)

    if len(valid_points) < 3:
        logging.debug(f"[{placa}] Apenas {len(valid_points)} pontos válidos")
        return 0

    logging.info(f"[{placa}] Analisando {len(valid_points)} pontos válidos")

    # 2) Anti-spike mais brando (já existente)
    def _is_spike_reversal(idx, pts, jump_pp, win_sec, tol_band):
        if idx == 0 or idx >= len(pts): return False
        prev = float(pts[idx-1]["nv"]); curr = float(pts[idx]["nv"])
        if abs(curr - prev) < 10.0: return False  # mantém 10pp
        t0 = pts[idx]["t"]; limit = t0 + timedelta(seconds=SPIKE_REV_WIN_SEC)
        low, high = prev - SPIKE_TOL_BAND_PP, prev + SPIKE_TOL_BAND_PP
        j = idx + 1
        while j < len(pts) and pts[j]["t"] <= limit:
            nvj = float(pts[j]["nv"])
            if low <= nvj <= high:
                return True
            j += 1
        return False

    clean_points, dropped_spikes = [], 0
    for i, p in enumerate(valid_points):
        if _is_spike_reversal(i, valid_points, 10.0, SPIKE_REV_WIN_SEC, SPIKE_TOL_BAND_PP):
            dropped_spikes += 1
            continue
        clean_points.append(p)
    if dropped_spikes:
        logging.info(f"[{placa}] Anti-spike removeu {dropped_spikes} pontos.")
    valid_points = clean_points

    time_window_all = deque()
    LONG_WINDOW_SEC = 600

    level_tracker = {"start_nv": None, "start_t": None, "current_nv": None, "current_t": None}

    for idx, point in enumerate(valid_points):
        # GEOfence/Retomar-Parado: se estamos bloqueados, só liberamos ao detectar <= SPEED_STOP_MAX_KMH
        if block_until_stopped:
            v = float(point.get("v") or 0)
            if v <= SPEED_STOP_MAX_KMH:
                if RESUME_STOP_DWELL_SEC > 0:
                    if stop_since_t is None:
                        stop_since_t = point["t"]
                    elif (point["t"] - stop_since_t).total_seconds() >= RESUME_STOP_DWELL_SEC:
                        block_until_stopped = False
                        stop_since_t = None
                        level_tracker = {"start_nv": point["nv"], "start_t": point["t"], "current_nv": point["nv"], "current_t": point["t"]}
                    else:
                        # ainda acumulando dwell parado
                        continue
                else:
                    block_until_stopped = False
                    level_tracker = {"start_nv": point["nv"], "start_t": point["t"], "current_nv": point["nv"], "current_t": point["t"]}
            # se ainda não parou, ignora esse ponto completamente
            if block_until_stopped:
                continue

        # Atualiza janela longa
        time_window_all.append((point["t"], float(point["nv"])))
        while (time_window_all[-1][0] - time_window_all[0][0]).total_seconds() > LONG_WINDOW_SEC:
            time_window_all.popleft()

        if level_tracker["start_nv"] is None:
            level_tracker["start_nv"] = point["nv"]
            level_tracker["start_t"] = point["t"]

        level_tracker["current_nv"] = point["nv"]
        level_tracker["current_t"] = point["t"]

        delta_accumulated = float(level_tracker["current_nv"] - level_tracker["start_nv"])
        time_elapsed = (level_tracker["current_t"] - level_tracker["start_t"]).total_seconds()

        # Abrir sessão ao detectar variação acumulada significativa
        if abs(delta_accumulated) >= 3.0 and time_elapsed >= 120 and state == "STABLE":
            tipo = "DESCARGA" if delta_accumulated < 0 else "COLETA"
            logging.info(f"[{placa}] Tendência: {delta_accumulated:.2f}pp/{time_elapsed:.0f}s - Iniciando {tipo}")
            sid = sessao_abrir(cur, placa, tipo, level_tracker["start_t"], level_tracker["start_nv"],
                               point.get("lat"), point.get("lon"))
            if sid:
                open_sess = {
                    "id": sid, "placa": placa, "tipo": tipo,
                    "t0": level_tracker["start_t"],
                    "nivel0": level_tracker["start_nv"],
                    "last_touch_t": point["t"],
                    "last_nivel": point["nv"],
                    "point_count": 1,
                    "last_unique_nv": point["nv"],
                    "lat0": point.get("lat"),
                    "lon0": point.get("lon"),
                }
                state = "TRENDING_DOWN" if tipo == "DESCARGA" else "TRENDING_UP"

        # Se há sessão aberta, tratar timeouts e GEOfence
        if open_sess:
            # GEOfence: se saiu do raio medido a partir do início, fecha/cancela e bloqueia até parar
            dist_m = _haversine_m(open_sess.get("lat0"), open_sess.get("lon0"), point.get("lat"), point.get("lon"))
            if dist_m > EXIT_RADIUS_M:
                logging.warning(f"[{placa}] Saiu do raio de {EXIT_RADIUS_M} m (dist={dist_m:.1f} m). Finalizando sessão {open_sess['id']} e aguardando parada.")
                finalizados += finalizar_sessao_se_valida(cur, open_sess)
                state = "STABLE"
                open_sess = None
                block_until_stopped = True
                stop_since_t = None
                # não reseta level_tracker aqui; só retomamos quando o veículo parar
                continue

            # Timeout de duração
            session_duration_min = (point["t"] - open_sess["t0"]).total_seconds() / 60
            if session_duration_min > MAX_SESSION_DURATION_MIN:
                logging.warning(f"[{placa}] Sessão {open_sess['id']} > {MAX_SESSION_DURATION_MIN} min - forçando fechamento")
                finalizados += finalizar_sessao_se_valida(cur, open_sess)
                state = "STABLE"
                open_sess = None
                level_tracker = {"start_nv": point["nv"], "start_t": point["t"], "current_nv": point["nv"], "current_t": point["t"]}
                continue

            # Timeout de “stale”
            if open_sess["last_touch_t"]:
                stale_time_min = (point["t"] - open_sess["last_touch_t"]).total_seconds() / 60
                if stale_time_min > MAX_STALE_TIME_MIN and abs(float(point["nv"] - open_sess["last_nivel"])) < 0.5:
                    logging.warning(f"[{placa}] Sessão {open_sess['id']} sem variação por {stale_time_min:.1f} min - finalizando")
                    finalizados += finalizar_sessao_se_valida(cur, open_sess)
                    state = "STABLE"
                    open_sess = None
                    level_tracker = {"start_nv": point["nv"], "start_t": point["t"], "current_nv": point["nv"], "current_t": point["t"]}
                    continue

            # Critérios de inversão
            delta_from_start = float(point["nv"] - open_sess["nivel0"])
            if open_sess["tipo"] == "DESCARGA":
                if delta_from_start > 2.0:
                    logging.info(f"[{placa}] DESCARGA interrompida (subiu {delta_from_start:.2f}pp)")
                    finalizados += finalizar_sessao_se_valida(cur, open_sess)
                    state = "STABLE"
                    open_sess = None
                    level_tracker = {"start_nv": point["nv"], "start_t": point["t"], "current_nv": point["nv"], "current_t": point["t"]}
                    continue
            else:  # COLETA
                if delta_from_start < -2.0:
                    logging.info(f"[{placa}] COLETA interrompida (caiu {delta_from_start:.2f}pp)")
                    finalizados += finalizar_sessao_se_valida(cur, open_sess)
                    state = "STABLE"
                    open_sess = None
                    level_tracker = {"start_nv": point["nv"], "start_t": point["t"], "current_nv": point["nv"], "current_t": point["t"]}
                    continue

            # Touch condicionado a “parado” (já existe a flag)
            should_touch = True
            if TOUCH_ONLY_WHEN_STOPPED:
                try:
                    v = float(point.get("v") or 0)
                    if v > SPEED_STOP_MAX_KMH:
                        should_touch = False
                except:
                    pass

            if should_touch:
                sessao_touch(cur, open_sess["id"], point["t"], point["nv"], point.get("lat"), point.get("lon"))
                open_sess["last_touch_t"] = point["t"]
                open_sess["last_nivel"] = point["nv"]
                if point["nv"] != open_sess["last_unique_nv"]:
                    open_sess["point_count"] += 1
                    open_sess["last_unique_nv"] = point["nv"]

        # Sem sessão aberta: se já passou muito tempo desde o start do tracker, reinicie a base
        elif not open_sess and time_elapsed > 300:
            level_tracker = {"start_nv": point["nv"], "start_t": point["t"], "current_nv": point["nv"], "current_t": point["t"]}

    # Finalização no fim da análise
    if open_sess:
        logging.info(f"[{placa}] Finalizando sessão aberta ao fim da análise")
        finalizados += finalizar_sessao_se_valida(cur, open_sess)

    return finalizados
# ====================== HTTP / API ======================
def _log_http_debug(resp, label="HTTP"):
    if not DEBUG_HTTP: return
    ctype = (resp.headers.get("Content-Type") or "").lower()
    body_preview = (resp.text or "")[:300].replace("\n", "\\n")
    req_body_len = len(resp.request.body) if resp.request and resp.request.body else 0
    logging.info(
        f"{label} {resp.request.method} {resp.url} -> status={resp.status_code} "
        f"req_len={req_body_len} resp_len={len(resp.content)} body^300={body_preview!r}"
    )

def login():
    url = _build_url(API_BASE_URL, AUTH_LOGIN_PATH)
    params = {AUTH_QUERY_USER_KEY: AUTH_USER, AUTH_QUERY_PASS_KEY: AUTH_PASS}
    if AUTH_QUERY_HASH_KEY and AUTH_HASH:
        params[AUTH_QUERY_HASH_KEY] = AUTH_HASH

    headers = {"Accept": "application/json"}
    if AUTH_METHOD == "GET_PARAMS":
        resp = requests.get(url, params=params, headers=headers, timeout=30)
    elif AUTH_METHOD == "POST_FORM":
        resp = requests.post(url, data=params, headers=headers, timeout=30)
    else:  # POST_PARAMS
        resp = requests.post(url, params=params, headers=headers, timeout=30)

    if not (200 <= resp.status_code < 300):
        raise RuntimeError(f"Login HTTP {resp.status_code}: {resp.text[:200]!r}")

    try:
        data = resp.json()
        token = data.get("AccessToken") if isinstance(data, dict) else str(data)
    except JSONDecodeError:
        token = resp.text.strip()
    
    if not token or "<" in token:
        raise RuntimeError(f"Token inválido recebido: {token[:120]!r}")
    return token

def api_list_positions(token: str, placa: str, dt_ini: datetime, dt_fim: datetime) -> list[dict]:
    url = _build_url(API_BASE_URL, GET_LAST_POSITIONS_PATH)
    headers = {AUTH_HEADER_NAME: AUTH_HEADER_TEMPLATE.format(token=token), "Accept": "application/json", "Content-Type": "application/json"}
    body = {
        "TrackedUnitType": 1,
        "TrackedUnitIntegrationCode": placa,
        "StartDatePosition": _to_iso_z(dt_ini),
        "EndDatePosition": _to_iso_z(dt_fim),
    }
    if CLIENT_INTEGRATION_CODE:
        body["ClientIntegrationCode"] = str(CLIENT_INTEGRATION_CODE)

    for tentativa in range(3):
        try:
            resp = requests.post(url, json=body, headers=headers, timeout=120)
            _log_http_debug(resp, label="HistoryPosition")

            if resp.status_code in (401, 403) and tentativa == 0:
                logging.warning("Auth expirada, tentando relogar...")
                token = login()
                headers[AUTH_HEADER_NAME] = AUTH_HEADER_TEMPLATE.format(token=token)
                continue
            
            if resp.status_code == 204: return []
            resp.raise_for_status()
            
            return resp.json() or []
        
        except (requests.RequestException, JSONDecodeError) as e:
            logging.error(f"API call falhou (tentativa {tentativa+1}): {e}")
            time.sleep(2 ** tentativa)
    return []

# ====================== Inserção em rastreio.posicao ======================
def inserir_posicoes(cur, linhas):
    if not linhas: return
    cols = ("id_position","placa","id_event","ignicao","valid_gps","data_evento",
            "data_atualizacao","latitude","longitude","inputs","outputs",
            "telemetria","nivel_tanque_percent","raw")
    tpl = f'({",".join(["%s"] * len(cols))})'
    sql = f"INSERT INTO rastreio.posicao ({','.join(cols)}) VALUES %s ON CONFLICT (id_position) DO NOTHING;"
    
    data_tuples = [
        tuple(linha.get(c) for c in cols)
        for linha in linhas
    ]
    execute_values(cur, sql, data_tuples, template=tpl, page_size=10000)

# ====================== ETL principal ======================
def coletar_e_gravar():
    token = login()
    with obter_conexao() as conn, conn.cursor() as cur:
        placas = sorted(carregar_placas_validas(cur))
        agora = datetime.now(timezone.utc)

        for placa in placas:
            try:
                dt_ultimo = obter_ultima_data_posicao(cur, placa)
                if dt_ultimo:
                    dt_ini = (dt_ultimo.astimezone(timezone.utc) if dt_ultimo.tzinfo else dt_ultimo.replace(tzinfo=LOCAL_TZ)).astimezone(timezone.utc)
                else:
                    dt_inst = obter_data_instalacao(cur, placa)
                    dt_ini = dt_inst.astimezone(timezone.utc) if dt_inst else datetime(agora.year, 1, 1, tzinfo=timezone.utc)
                
                dt_fim = agora
                if dt_ini >= dt_fim:
                    logging.info(f"[{placa}] Sem novas posições para buscar (última em {dt_ultimo}).")
                    continue

                # ===== Paginação por tempo (time-cursor) =====
                logging.info(f"[{placa}] Buscando janela de {_to_iso_z(dt_ini)} até {_to_iso_z(dt_fim)}")

                SOFT_CAP = 1000  # limite observado na API (ajuste se mudar)
                cursor_ini = dt_ini
                total_payload = 0
                candidatos = []

                while cursor_ini < dt_fim:
                    lote = api_list_positions(token, placa, cursor_ini, dt_fim)
                    n = len(lote)
                    logging.info(f"[{placa}] Lote API: {n} posições (cursor={_to_iso_z(cursor_ini)} .. {_to_iso_z(dt_fim)})")
                    if n == 0:
                        break

                    # Ordena por timestamp da API (asc) para pegar o último com precisão de ms
                    try:
                        lote.sort(key=lambda x: _parse_dt_any(x.get("EventDate")))
                    except Exception:
                        pass

                    # Converte lote em 'candidatos' (mesma lógica original)
                    for item in lote:
                        try:
                            idp = int(item["IdPosition"])
                            tele = item.get("ListTelemetry") or {}
                            nivel_raw = item.get("PercentageLevelTank")
                            if nivel_raw is None and isinstance(tele, dict):
                                nivel_raw = tele.get("304")
                            vel = item.get("SpeedKmh")
                            if vel is None:
                                vel = item.get("Speed")
                            candidatos.append({
                                "id_position": idp, "placa": placa, "id_event": item.get("IdEvent"),
                                "ignicao": item.get("Ignition"), "valid_gps": item.get("ValidGPS"),
                                "data_evento": _to_db_ts(item.get("EventDate")),
                                "data_atualizacao": _to_db_ts(item.get("UpdateDate")) if item.get("UpdateDate") else None,
                                "latitude": item.get("Latitude"), "longitude": item.get("Longitude"),
                                "inputs": json.dumps(item.get("ListInputSensor") or {}),
                                "outputs": json.dumps(item.get("ListOutputActuator") or {}),
                                "telemetria": json.dumps(tele),
                                "nivel_tanque_percent": float(nivel_raw) if nivel_raw is not None else None,
                                "velocidade_kmh": float(vel) if vel is not None else None,
                                "raw": json.dumps(item),
                            })
                        except (KeyError, TypeError, ValueError) as e:
                            logging.warning(f"Erro ao processar item para {placa}: {e} - Item: {str(item)[:200]}")

                    total_payload += n

                    # Se bateu perto do limite, avança o cursor para DEPOIS do último timestamp do lote
                    if n >= SOFT_CAP:
                        last_api_ts_utc = _parse_dt_any(lote[-1].get("EventDate"))  # aware/UTC
                        # avança 1 ms para não reprocessar o último
                        cursor_ini = (last_api_ts_utc + timedelta(milliseconds=1)).astimezone(timezone.utc)
                        continue
                    else:
                        # não bateu o cap -> já consumimos tudo
                        break

                logging.info(f"[{placa}] Total recebido (paginado): {total_payload}")

                # ===== dedupe + insert exatamente como você já fazia =====
                if not candidatos:
                    continue

                mapa = {c["id_position"]: c for c in candidatos}
                ids = list(mapa.keys())
                existentes = carregar_ids_existentes(cur, ids)
                linhas_novas = [c for c in mapa.values() if c["id_position"] not in existentes]

                if not linhas_novas:
                    logging.info(f"[{placa}] Nenhuma posição nova após filtrar existentes.")
                    continue

                linhas_novas.sort(key=lambda r: (r["data_evento"], r["id_position"]))
                inserir_posicoes(cur, linhas_novas)
                logging.info(f"[{placa}] Inseridas {len(linhas_novas)} novas posições.")
                conn.commit()

                LOOKBACK_MINUTES = int(os.getenv("LOOKBACK_MINUTES", "30"))
                total_sessoes = detect_events_with_context(cur, placa, linhas_novas, lookback_minutes=LOOKBACK_MINUTES)
                if total_sessoes > 0:
                    logging.info(f"[{placa}] Finalizadas {total_sessoes} sessões neste ciclo.")
                conn.commit()


            except Exception as e:
                logging.exception(f"Falha crítica no processamento da placa {placa}: {e}")

        cur.execute("SELECT operacao.fechar_sessoes_stagnadas(%s);", (int(GAP_MIN),))
        rows_closed = cur.rowcount
        if rows_closed > 0:
            logging.info(f"Finalizadas {rows_closed} sessões estagnadas por GAP.")
        conn.commit()

# ====================== Loop Principal ======================
def loop():
    while True:
        try:
            coletar_e_gravar()
        except Exception as e:
            logging.exception(f"Falha irrecuperável no ciclo de ETL: {e}")
        logging.info(f"Aguardando {FREQUENCIA} segundos para o próximo ciclo.")
        time.sleep(FREQUENCIA)

if __name__ == "__main__":
    loop()