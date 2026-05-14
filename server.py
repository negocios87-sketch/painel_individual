"""
Board Academy — Dashboard SDR
Servidor Python + Flask para Render

Variáveis de ambiente necessárias no Render:
    PIPEDRIVE_KEY = sua_chave_aqui
    SECRET_KEY    = qualquer_string_aleatoria
"""

from flask import Flask, jsonify, request, session, redirect, send_from_directory
import requests as req
import pandas as pd
import os
import unicodedata
from datetime import date, datetime, timedelta
from io import StringIO
import math

def limpar_nans(obj):
    if isinstance(obj, dict):
        return {k: limpar_nans(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [limpar_nans(v) for v in obj]
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return 0
    return obj
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

app = Flask(__name__, static_folder="static")
app.secret_key = os.getenv("SECRET_KEY", "boardacademy2026")

# ── CONFIG ──────────────────────────────────────────────────
API_KEY  = os.getenv("PIPEDRIVE_KEY")
BASE_V1  = "https://boardacademy.pipedrive.com/api/v1"
BASE_V2  = "https://boardacademy.pipedrive.com/api/v2"

GITHUB_FOTOS = "https://raw.githubusercontent.com/negocios87-sketch/fotos_time_comercial/main"

URL_COLAB = "https://docs.google.com/spreadsheets/d/e/2PACX-1vSvwO3Ag2f2cbkVgR1pJZp6fANQcbualGKlAG50fmOljuEGKZ1gJBbSAjRdO3SomXUEVQOWnTvlfHRd/pub?gid=1782440078&single=true&output=csv"
URL_METAS = "https://docs.google.com/spreadsheets/d/e/2PACX-1vSvwO3Ag2f2cbkVgR1pJZp6fANQcbualGKlAG50fmOljuEGKZ1gJBbSAjRdO3SomXUEVQOWnTvlfHRd/pub?gid=0&single=true&output=csv"
URL_OTE   = "https://docs.google.com/spreadsheets/d/e/2PACX-1vSvwO3Ag2f2cbkVgR1pJZp6fANQcbualGKlAG50fmOljuEGKZ1gJBbSAjRdO3SomXUEVQOWnTvlfHRd/pub?gid=569278086&single=true&output=csv"

URL_COMISSOES = "https://docs.google.com/spreadsheets/d/e/2PACX-1vSvwO3Ag2f2cbkVgR1pJZp6fANQcbualGKlAG50fmOljuEGKZ1gJBbSAjRdO3SomXUEVQOWnTvlfHRd/pub?gid=1720844569&single=true&output=csv"
URL_USERS = "https://docs.google.com/spreadsheets/d/e/2PACX-1vSvwO3Ag2f2cbkVgR1pJZp6fANQcbualGKlAG50fmOljuEGKZ1gJBbSAjRdO3SomXUEVQOWnTvlfHRd/pub?gid=160245570&single=true&output=csv"

FILTER_DEALS      = 1464384
FILTER_ACTIVITIES = 1310451
FILTER_DEALS_RV   = 1466157
FILTER_REFERIDOS  = 1562285
TABELA_PRICE_URL  = "https://inspiring-marshmallow-1ba.netlify.app/"

CF_MULTIPLICADOR = "7e0e43c2734751f77be292a72527f638a850ad50"
CF_QUALIFICADOR  = "a6f13cc27c8d041f3af4091283ce0d4fe0913875"
CF_REUNIAO_VALID = "7299bf170c5deab9b4fd8c2275f55faf51984dea"

RV_SIM    = "411"
RV_NAO    = "412"
RV_NOSHOW = "481"

TIMES_ESCOPO = ["elite", "sniper", "atlantis", "mgm", "orion", "latam", "zenite"]

# ── HELPERS ─────────────────────────────────────────────────
def norm(s):
    if not s:
        return ""
    s = str(s).strip().lower()
    return unicodedata.normalize("NFD", s).encode("ascii", "ignore").decode()

def arred(v):
    try:
        return round(float(v), 2)
    except:
        return 0.0

def acelerador(pct):
    if pct >= 120: return 1.5
    if pct >= 100: return 1.0
    if pct >= 86:  return 0.7
    if pct >= 60:  return 0.5
    return 0.0

def dias_uteis_passados():
    hoje = date.today()
    count = 0
    d = date(hoje.year, hoje.month, 1)
    while d <= hoje:
        if d.weekday() < 5:
            count += 1
        if d == hoje:
            break
        try:
            d = date(d.year, d.month, d.day + 1)
        except:
            break
    return max(count, 1)

def ajustar_hora(hora_str):
    if not hora_str:
        return None
    try:
        h, m = hora_str[:5].split(":")
        dt = datetime(2000, 1, 1, int(h), int(m)) - timedelta(hours=3)
        return dt.strftime("%H:%M")
    except:
        return hora_str

def url_foto(nome):
    base = f"{GITHUB_FOTOS}/{nome}"
    for ext in ["jpg", "jpeg", "png", "JPG", "JPEG", "PNG"]:
        url = f"{base}.{ext}"
        try:
            r = req.head(url, timeout=5)
            if r.status_code == 200:
                return url
        except:
            pass
    return None

# ── SHEETS ──────────────────────────────────────────────────
def ler_sheet(url):
    resp = req.get(url, timeout=15)
    resp.encoding = "utf-8"
    resp.raise_for_status()
    return pd.read_csv(StringIO(resp.text))

def buscar_usuario(usuario, senha):
    df = ler_sheet(URL_USERS)
    df.columns = [c.strip().lower() for c in df.columns]
    for _, row in df.iterrows():
        if (norm(str(row.get("usuario", ""))) == norm(usuario) and
            str(row.get("senha", "")).strip() == str(senha).strip()):
            return str(row.get("usuario", ""))
    return None

def buscar_colaborador(nome):
    df = ler_sheet(URL_COLAB)
    df.columns = [c.strip() for c in df.columns]
    hoje = date.today()
    ano_atual = hoje.year
    mes_atual = hoje.month

    # Detecta colunas de referência de mês/ano
    col_mes_ref = next((c for c in df.columns if "mes" in norm(c) and "ref" in norm(c)), None)
    col_ano_ref = next((c for c in df.columns if "ano" in norm(c) and "ref" in norm(c)), None)

    melhor = None
    for _, row in df.iterrows():
        if norm(str(row.get("Nome", ""))) != norm(nome):
            continue
        subarea = str(row.get("Subarea", ""))
        status  = str(row.get("Status (Euqipe Comercial)", ""))
        if norm(status) != "ativo" or norm(subarea) not in TIMES_ESCOPO:
            continue

        # Se tem colunas de referência, filtra pelo mês atual
        if col_mes_ref and col_ano_ref:
            try:
                mes_ref = int(float(str(row.get(col_mes_ref, 0))))
                ano_ref = int(float(str(row.get(col_ano_ref, 0))))
                if ano_ref == ano_atual and mes_ref == mes_atual:
                    return {
                        "nome":  str(row.get("Nome", "")),
                        "time":  subarea,
                        "cargo": str(row.get("Cargo", "")),
                    }
                # Guarda como fallback caso não encontre o mês exato
                melhor = {
                    "nome":  str(row.get("Nome", "")),
                    "time":  subarea,
                    "cargo": str(row.get("Cargo", "")),
                }
            except:
                melhor = {
                    "nome":  str(row.get("Nome", "")),
                    "time":  subarea,
                    "cargo": str(row.get("Cargo", "")),
                }
        else:
            # Sem colunas de referência, retorna primeira linha ativa
            return {
                "nome":  str(row.get("Nome", "")),
                "time":  subarea,
                "cargo": str(row.get("Cargo", "")),
            }

    return melhor

def buscar_metas(nome):
    df = ler_sheet(URL_METAS)
    df.columns = [c.strip() for c in df.columns]
    hoje = date.today()

    def limpar_numero(val):
        try:
            return float(str(val or "0").strip())
        except:
            return 0.0

    for _, row in df.iterrows():
        try:
            ano = int(float(str(row.get("Ano", 0))))
            mes = int(float(str(row.get("Mes", row.get("Mês", 0)))))
        except:
            continue
        if norm(row.get("Nome")) == norm(nome) and ano == hoje.year and mes == hoje.month:
            ramp_str = str(row.get("% de Rampagem", "1") or "1").replace("%","").strip()
            ramp_val = float(ramp_str)
            ramp = ramp_val / 100 if ramp_val > 1 else ramp_val
            return {
                "meta_reu":   limpar_numero(row.get("Meta de Reunioes", row.get("Meta de Reuniões", 0))),
                "meta_fin":   limpar_numero(row.get("Meta Financeira", 0)),
                "rampagem":   ramp,
                "dias_uteis": int(float(row.get("Dias Uteis", 20) or 20)),
            }
    return None

def buscar_ote(cargo):
    df = ler_sheet(URL_OTE)
    df.columns = [c.strip() for c in df.columns]
    for _, row in df.iterrows():
        nivel = str(row.get("Nivel", row.get("Nível", "")))
        if norm(nivel) == norm(cargo):
            return {
                "total":    float(row.get("ote_total", 0) or 0),
                "fixo":     float(row.get("fixo", 0) or 0),
                "variavel": float(row.get("variavel", 0) or 0),
            }
    return None

# ── PIPEDRIVE ────────────────────────────────────────────────
def buscar_qualificador_id(nome):
    resp = req.get(f"{BASE_V1}/dealFields",
        params={"api_token": API_KEY}, timeout=15)
    resp.raise_for_status()
    fields = resp.json().get("data") or []
    for field in fields:
        if field.get("key") == CF_QUALIFICADOR:
            for opt in (field.get("options") or []):
                if norm(opt.get("label","")) == norm(nome):
                    return str(opt.get("id"))
    return None

def buscar_users():
    resp = req.get(f"{BASE_V1}/users",
        params={"api_token": API_KEY}, timeout=15)
    resp.raise_for_status()
    mapa = {}
    for u in (resp.json().get("data") or []):
        mapa[u["id"]] = u["name"]
    return mapa

def encontrar_user_id(users, nome):
    for uid, uname in users.items():
        if norm(uname) == norm(nome):
            return uid
    return None

def buscar_deals():
    todos, start = [], 0
    while True:
        resp = req.get(f"{BASE_V1}/deals", params={
            "filter_id": FILTER_DEALS,
            "limit": 500,
            "start": start,
            "api_token": API_KEY
        }, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        lote = data.get("data") or []
        todos.extend(lote)
        mais = data.get("additional_data", {}).get("pagination", {}).get("more_items_in_collection", False)
        if not mais or not lote:
            break
        start += 500
    return todos

def buscar_activities():
    todos, cursor = [], None
    while True:
        params = {"filter_id": FILTER_ACTIVITIES, "limit": 200}
        if cursor:
            params["cursor"] = cursor
        resp = req.get(f"{BASE_V2}/activities",
            params=params,
            headers={"x-api-token": API_KEY},
            timeout=30)
        resp.raise_for_status()
        data   = resp.json()
        lote   = data.get("data") or []
        todos.extend(lote)
        cursor = data.get("additional_data", {}).get("next_cursor")
        if not cursor or not lote:
            break
    return todos

def buscar_deals_rv():
    """Busca deals com Reunião Validada? != Não e != No Show."""
    deal_ids_validos = set()
    mapa_owner = {}
    start = 0
    while True:
        resp = req.get(f"{BASE_V1}/deals", params={
            "filter_id": FILTER_DEALS_RV,
            "status": "all_not_deleted",
            "limit": 500,
            "start": start,
            "api_token": API_KEY,
        }, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        lote = data.get("data") or []
        for d in lote:
            did = d["id"]
            uid = d.get("user_id")
            deal_ids_validos.add(did)
            mapa_owner[did] = uid.get("id") if isinstance(uid, dict) else uid
        mais = data.get("additional_data", {}).get("pagination", {}).get("more_items_in_collection", False)
        if not mais or not lote:
            break
        start += 500
    return deal_ids_validos, mapa_owner

def cf(deal, key):
    val = deal.get(key)
    if val is None:
        return None
    if isinstance(val, dict):
        return val.get("value") or val.get("label")
    return val

# ── CÁLCULO ──────────────────────────────────────────────────
def calcular(nome, user_id, qualificador_id, colaborador, metas, ote, deals, activities):

    deals_ganhos = [
        d for d in deals
        if d.get("status") == "won" and str(cf(d, CF_QUALIFICADOR)) == str(qualificador_id)
    ]

    valor_bruto = sum(float(d.get("value") or 0) for d in deals_ganhos)
    valor_multi = sum(float(cf(d, CF_MULTIPLICADOR) or 0) for d in deals_ganhos)

    hoje      = date.today()
    mes_atual = hoje.strftime("%Y-%m")

    acts_sdr = [
        a for a in activities
        if str(a.get("owner_id")) == str(user_id)
        and str(a.get("due_date", ""))[:7] == mes_atual
    ]

    # Busca deals válidos (Reunião Validada? != Não e != No Show)
    deal_ids_validos, mapa_deal_owner = buscar_deals_rv()

    # Completa mapa_owner com deals ganhos do mês
    for d in deals:
        did = d["id"]
        if did not in mapa_deal_owner:
            uid = d.get("user_id")
            mapa_deal_owner[did] = uid.get("id") if isinstance(uid, dict) else uid

    def valida(a):
        if not (a.get("done") == True or a.get("status") == "done"):
            return False
        deal_id = a.get("deal_id")
        # SDR não pode agendar reunião para si mesmo
        act_owner  = str(a.get("owner_id", ""))
        deal_owner = str(mapa_deal_owner.get(deal_id, "")) if deal_id else ""
        if act_owner and deal_owner and act_owner == deal_owner:
            return False
        # Deal deve estar no filtro de reuniões válidas
        if deal_id and deal_id not in deal_ids_validos:
            return False
        return True

    def eh_desq(a):
        if not (a.get("done") == True or a.get("status") == "done"):
            return False
        deal_id = a.get("deal_id")
        if not deal_id:
            return False
        return deal_id not in deal_ids_validos

    reu_realizadas = [a for a in acts_sdr if valida(a)]
    reu_desq       = [a for a in acts_sdr if eh_desq(a)]

    qtd_agendadas  = len(acts_sdr)
    qtd_realizadas = len(reu_realizadas)

    pct_reu = (qtd_realizadas / metas["meta_reu"]) if metas["meta_reu"] > 0 else 0
    pct_fin = (valor_multi / metas["meta_fin"])    if metas["meta_fin"] > 0 else 0
    ating   = (pct_reu * 0.7 + pct_fin * 0.3) * 100

    acel     = acelerador(ating)
    comissao = ote["variavel"] * (ating / 100) * metas["rampagem"] * acel

    dp = dias_uteis_passados()
    du = metas["dias_uteis"]
    proj_fin      = (valor_multi / dp) * du
    proj_reu      = (qtd_realizadas / dp) * du
    pct_proj_fin  = (proj_fin / metas["meta_fin"] * 100) if metas["meta_fin"] > 0 else 0
    pct_proj_reu  = (proj_reu / metas["meta_reu"] * 100)  if metas["meta_reu"] > 0 else 0
    ating_proj    = (pct_proj_reu * 0.7 + pct_proj_fin * 0.3)
    acel_proj     = acelerador(ating_proj)
    comissao_proj = ote["variavel"] * (ating_proj / 100) * metas["rampagem"] * acel_proj

    def serie_acts(lista):
        mapa = {}
        for a in lista:
            dt = str(a.get("due_date", ""))[:10]
            if dt:
                mapa[dt] = mapa.get(dt, 0) + 1
        return [{"data": k, "qtd": v} for k, v in sorted(mapa.items())]

    def serie_deals(lista):
        mapa = {}
        for d in lista:
            dt = str(d.get("won_time", ""))[:10]
            if dt:
                v = float(cf(d, CF_MULTIPLICADOR) or 0)
                mapa[dt] = mapa.get(dt, 0) + v
        return [{"data": k, "valor": v} for k, v in sorted(mapa.items())]

    proximas = sorted(
        [a for a in acts_sdr if not (a.get("done") or a.get("status") == "done")],
        key=lambda a: (a.get("due_date", ""), a.get("due_time", "") or "")
    )[:10]

    foto = url_foto(nome)

    return {
        "colaborador": {**colaborador, "foto": foto},
        "metas":       metas,
        "ote":         ote,
        "kpis": {
            "reunioesAgendadas":  qtd_agendadas,
            "reunioesRealizadas": qtd_realizadas,
            "pctMetaReu":         arred(pct_reu * 100),
            "valorBruto":         arred(valor_bruto),
            "valorMultiplicador": arred(valor_multi),
            "pctMetaFin":         arred(pct_fin * 100),
            "pctDesqualificacao": arred(len(reu_desq) / qtd_realizadas * 100) if qtd_realizadas else 0,
        },
        "comissao": {
            "atingimento":   arred(ating),
            "acelerador":    acel,
            "estimada":      arred(comissao),
            "comissionando": ating >= 60,
            "projecao": {
                "valorFin":    arred(proj_fin),
                "qtdReu":      arred(proj_reu),
                "atingimento": arred(ating_proj),
                "estimada":    arred(comissao_proj),
            }
        },
        "graficos": {
            "serieAgendadas":  serie_acts(acts_sdr),
            "serieRealizadas": serie_acts(reu_realizadas),
            "serieValores":    serie_deals(deals_ganhos),
        },
        "tabelas": {
            "proximasReunioes": [{"deal_id": a.get("deal_id"), "nome": a.get("subject", ""), "data": a.get("due_date"), "hora": ajustar_hora(a.get("due_time"))} for a in proximas],
            "reunioesGanhas":   [{"deal_id": d["id"], "nome": d.get("title"), "data_ganho": str(d.get("won_time", ""))[:10], "valor_bruto": float(d.get("value") or 0), "valor_multi": float(cf(d, CF_MULTIPLICADOR) or 0)} for d in deals_ganhos[:10]],
            "reunioesDesq": [{
                "deal_id":      a.get("deal_id"),
                "proprietario": next((d.get("owner_name") or (d.get("user_id") or {}).get("name","") for d in deals if d["id"] == a.get("deal_id")), "--")
            } for a in reu_desq[:10]],
        },
        "atualizadoEm": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
    }



def is_closer(cargo):
    """Retorna True se o cargo é Closer."""
    return "closer" in norm(cargo)

def buscar_referidos():
    """Busca deals do filtro de referidos (mês atual)."""
    todos, start = [], 0
    mes_atual = date.today().strftime("%Y-%m")
    while True:
        resp = req.get(f"{BASE_V1}/deals", params={
            "filter_id": FILTER_REFERIDOS,
            "limit": 500,
            "start": start,
            "api_token": API_KEY
        }, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        lote = data.get("data") or []
        for d in lote:
            add_time = str(d.get("add_time", ""))[:7]
            if add_time == mes_atual:
                todos.append(d)
        mais = data.get("additional_data", {}).get("pagination", {}).get("more_items_in_collection", False)
        if not mais or not lote:
            break
        start += 500
    return todos

# ── CÁLCULO CLOSER ───────────────────────────────────────────
def calcular_closer(nome, user_id, colaborador, metas, ote, deals, activities, referidos):
    hoje      = date.today()
    mes_atual = hoje.strftime("%Y-%m")

    # Deals ganhos onde ele é proprietário
    deals_ganhos = [
        d for d in deals
        if d.get("status") == "won"
        and str((d.get("user_id") or {}).get("id") if isinstance(d.get("user_id"), dict) else d.get("user_id")) == str(user_id)
    ]

    valor_bruto = sum(float(d.get("value") or 0) for d in deals_ganhos)
    valor_multi = sum(float(cf(d, CF_MULTIPLICADOR) or 0) for d in deals_ganhos)

    # Mapa deal_id -> "Reunião Validada?"
    deal_ids_validos, mapa_deal_owner = buscar_deals_rv()

    # Achar user_id do Matheus Paz
    users_pipe = buscar_users()
    matheus_id = str(next((uid for uid, uname in users_pipe.items() if norm(uname) == norm("Matheus Paz")), ""))

    # Times Inside Sales — podem agendar para si mesmos
    TIMES_INSIDE_SALES = ["orion", "latam"]
    is_inside_sales = norm(colaborador.get("time", "")) in TIMES_INSIDE_SALES

    # Activities do mês onde o closer é owner do DEAL (não da activity)
    acts_closer = [
        a for a in activities
        if str(a.get("due_date", ""))[:7] == mes_atual
        and str(mapa_deal_owner.get(a.get("deal_id"), "")) == str(user_id)
        and (is_inside_sales or str(a.get("owner_id", "")) != str(user_id))
        and (not matheus_id or str(a.get("owner_id", "")) != matheus_id)
    ]

    # Realizadas = done
    reu_realizadas = [
        a for a in acts_closer
        if a.get("done") == True or a.get("status") == "done"
    ]

    # Validadas = realizadas + Reunião Validada? = Sim
    reu_validadas = [
        a for a in reu_realizadas
        if a.get("deal_id") and a.get("deal_id") in deal_ids_validos
    ]

    qtd_realizadas = len(reu_realizadas)
    qtd_validadas  = len(reu_validadas)
    pct_validadas  = arred(qtd_validadas / qtd_realizadas * 100) if qtd_realizadas else 0

    # Referidos do mês onde ele é proprietário
    refs_closer = [
        d for d in referidos
        if str((d.get("user_id") or {}).get("id") if isinstance(d.get("user_id"), dict) else d.get("user_id")) == str(user_id)
    ]

    # % Meta financeira
    pct_fin = (valor_multi / metas["meta_fin"]) if metas["meta_fin"] > 0 else 0
    ating   = pct_fin * 100

    acel     = acelerador(ating)
    comissao = ote["variavel"] * (ating / 100) * metas["rampagem"] * acel

    # Projeção
    dp = dias_uteis_passados()
    du = metas["dias_uteis"]
    proj_fin     = (valor_multi / dp) * du
    pct_proj_fin = (proj_fin / metas["meta_fin"] * 100) if metas["meta_fin"] > 0 else 0
    acel_proj    = acelerador(pct_proj_fin)
    comissao_proj = ote["variavel"] * (pct_proj_fin / 100) * metas["rampagem"] * acel_proj

    def serie_acts(lista):
        mapa = {}
        for a in lista:
            dt = str(a.get("due_date", ""))[:10]
            if dt:
                mapa[dt] = mapa.get(dt, 0) + 1
        return [{"data": k, "qtd": v} for k, v in sorted(mapa.items())]

    def serie_deals(lista):
        mapa = {}
        for d in lista:
            dt = str(d.get("won_time", ""))[:10]
            if dt:
                v = float(cf(d, CF_MULTIPLICADOR) or 0)
                mapa[dt] = mapa.get(dt, 0) + v
        return [{"data": k, "valor": v} for k, v in sorted(mapa.items())]

    def serie_referidos(lista):
        mapa = {}
        for d in lista:
            dt = str(d.get("add_time", ""))[:10]
            if dt:
                mapa[dt] = mapa.get(dt, 0) + 1
        return [{"data": k, "qtd": v} for k, v in sorted(mapa.items())]

    def owner_name(d):
        uid = d.get("user_id")
        if isinstance(uid, dict):
            return uid.get("name", "--")
        return "--"

    foto = url_foto(nome)

    return {
        "tipo": "closer",
        "colaborador": {**colaborador, "foto": foto},
        "metas": metas,
        "ote":   ote,
        "kpis": {
            "reunioesRealizadas": qtd_realizadas,
            "reunioesValidadas":  qtd_validadas,
            "pctValidadas":       pct_validadas,
            "valorBruto":         arred(valor_bruto),
            "valorMultiplicador": arred(valor_multi),
            "pctMetaFin":         arred(pct_fin * 100),
            "volumeReferidos":    len(refs_closer),
        },
        "comissao": {
            "atingimento":   arred(ating),
            "acelerador":    acel,
            "estimada":      arred(comissao),
            "comissionando": ating >= 60,
            "projecao": {
                "valorFin":    arred(proj_fin),
                "atingimento": arred(pct_proj_fin),
                "estimada":    arred(comissao_proj),
            }
        },
        "graficos": {
            "serieRealizadas": serie_acts(reu_realizadas),
            "serieValores":    serie_deals(deals_ganhos),
            "serieReferidos":  serie_referidos(refs_closer),
        },
        "tabelas": {
            "reunioesRealizadas": [{"deal_id": a.get("deal_id"), "nome": a.get("subject",""), "data": a.get("due_date"), "hora": ajustar_hora(a.get("due_time"))} for a in reu_realizadas[:10]],
            "ganhos": [{"deal_id": d["id"], "nome": d.get("title"), "data_ganho": str(d.get("won_time",""))[:10], "valor_bruto": float(d.get("value") or 0), "valor_multi": float(cf(d, CF_MULTIPLICADOR) or 0), "closer": owner_name(d)} for d in deals_ganhos[:10]],
            "referidos": [{"deal_id": d["id"], "nome": d.get("title"), "data_criacao": str(d.get("add_time",""))[:10], "valor_bruto": float(d.get("value") or 0), "valor_multi": float(cf(d, CF_MULTIPLICADOR) or 0), "closer": owner_name(d)} for d in refs_closer[:10]],
        },
        "tabela_price_url": TABELA_PRICE_URL,
        "atualizadoEm": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
    }

# ── COMISSÃO ────────────────────────────────────────────────
def buscar_comissoes(nome):
    df = ler_sheet(URL_COMISSOES)
    df.columns = [c.strip() for c in df.columns]
    registros = []
    for _, row in df.iterrows():
        if norm(str(row.get("Nome", ""))) != norm(nome):
            continue
        registros.append({
            "ano":            str(row.get("Ano", "")),
            "mes":            str(row.get("Mês", row.get("Mes", ""))),
            "nivel":          str(row.get("Nível", row.get("Nivel", ""))),
            "vol_reuniao":    str(row.get("Vol. Reunião", row.get("Vol. Reuniao", ""))),
            "valor_financeiro": str(row.get("Valor Financeiro", "")),
            "pct_reuniao":    str(row.get("% Reunião", row.get("% Reuniao", ""))),
            "pct_financeiro": str(row.get("% Financeiro", "")),
            "pct_meta_final": str(row.get("% Meta Final", "")),
            "elegivel":       str(row.get("Elegível?", row.get("Elegivel?", ""))),
            "acelerador":     str(row.get("Acelerador OTE", "")),
            "variavel_ote":   str(row.get("Variável OTE", row.get("Variavel OTE", ""))),
            "rampagem":       str(row.get("Rampagem (OTE)", "")),
            "comissao":       str(row.get("Comissão", row.get("Comissao", ""))),
            "confirmacao":    str(row.get("Confirmação", row.get("Confirmacao", ""))).strip(),
        })
    # Ordem: mais recente primeiro
    registros.sort(key=lambda r: (r["ano"], r["mes"].zfill(2)), reverse=True)
    return registros

def gravar_confirmacao(nome, ano, mes):
    import gspread
    # Fallback: usar requests para atualizar via Sheets API não disponível sem OAuth
    # Usamos uma abordagem alternativa: webhook para um Google Apps Script
    # Por ora, retorna erro indicando que precisa configurar o Apps Script
    return False, "Configure o endpoint de gravação"


@app.route("/api/comissao/confirmar", methods=["POST"])
def api_confirmar_comissao():
    if "nome" not in session:
        return jsonify({"erro": "Não autenticado"}), 401
    nome = session["nome"]
    body = request.get_json()
    ano   = str(body.get("ano", ""))
    mes   = str(body.get("mes", ""))
    valor = str(body.get("valor", ""))
    agora = (datetime.now() - timedelta(hours=3)).strftime("%d/%m/%Y %H:%M")
    enviar_notificacao(nome, mes, ano, valor, agora)
    return jsonify({"ok": True})

@app.route("/comissao")
def comissao():
    if "nome" not in session:
        return redirect("/login")
    return send_from_directory(".", "comissao.html")

@app.route("/api/comissao")
def api_comissao():
    if "nome" not in session:
        return jsonify({"erro": "Não autenticado"}), 401
    nome = session["nome"]
    try:
        registros = buscar_comissoes(nome)
        return jsonify({"nome": nome, "registros": registros})
    except Exception as e:
        import traceback
        return jsonify({"erro": str(e), "trace": traceback.format_exc()}), 500




def enviar_notificacao(nome, mes, ano, valor, data_hora):
    """Envia notificação via Power Automate webhook."""
    try:
        webhook_url = os.getenv("POWERAUTOMATE_WEBHOOK", "")
        if not webhook_url:
            print("POWERAUTOMATE_WEBHOOK não configurada")
            return False

        nomes_meses = ['','Janeiro','Fevereiro','Março','Abril','Maio','Junho',
                       'Julho','Agosto','Setembro','Outubro','Novembro','Dezembro']
        mes_nome = nomes_meses[int(mes)] if str(mes).isdigit() else mes

        payload = {
            "Nome":      nome,
            "Periodo":   f"{mes_nome} / {ano}",
            "Valor":     f"R$ {valor}",
            "DataHora":  data_hora
        }

        r = req.post(webhook_url, json=payload, timeout=15)
        print(f"Power Automate status: {r.status_code}")
        return r.status_code in (200, 201, 202)
    except Exception as e:
        import traceback
        print(f"ERRO WEBHOOK: {e}")
        traceback.print_exc()
        return False

# ── ROTAS ────────────────────────────────────────────────────
@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        usuario = request.form.get("usuario", "").strip()
        senha   = request.form.get("senha", "").strip()
        nome    = buscar_usuario(usuario, senha)
        if nome:
            session["nome"] = nome
            return redirect("/dashboard")
        return send_from_directory(".", "login.html"), 401
    return send_from_directory(".", "login.html")

@app.route("/logout")
def logout():
    session.clear()
    return redirect("/login")

@app.route("/dashboard")
def dashboard():
    if "nome" not in session:
        return redirect("/login")
    return send_from_directory(".", "dashboard.html")

@app.route("/api/sdr")
def api_sdr():
    if "nome" not in session:
        return jsonify({"erro": "Não autenticado"}), 401
    nome = session["nome"]
    try:
        colaborador = buscar_colaborador(nome)
        if not colaborador:
            return jsonify({"erro": f"'{nome}' nao encontrado ou inativo"}), 404

        metas = buscar_metas(nome)
        if not metas:
            return jsonify({"erro": f"Metas de '{nome}' nao encontradas para o mes atual"}), 404

        ote = buscar_ote(colaborador["cargo"])
        if not ote:
            return jsonify({"erro": f"OTE nao encontrado para cargo '{colaborador['cargo']}'"}), 404

        users           = buscar_users()
        user_id         = encontrar_user_id(users, nome)
        if not user_id:
            return jsonify({"erro": f"Usuario '{nome}' nao encontrado no Pipedrive"}), 404

        deals           = buscar_deals()
        activities      = buscar_activities()
        qualificador_id = buscar_qualificador_id(nome)

        resultado = calcular(nome, user_id, qualificador_id, colaborador, metas, ote, deals, activities)
        return jsonify(limpar_nans(resultado))

    except Exception as e:
        import traceback
        return jsonify({"erro": str(e), "trace": traceback.format_exc()}), 500

@app.route("/")
def index():
    return redirect("/login")

@app.route("/debug/erro")
def debug_erro():
    if "nome" not in session:
        return jsonify({"erro": "não autenticado"}), 401
    try:
        nome = session["nome"]
        colaborador = buscar_colaborador(nome)
        metas = buscar_metas(nome)
        ote = buscar_ote(colaborador["cargo"]) if colaborador else None
        users = buscar_users()
        user_id = encontrar_user_id(users, nome)
        deals = buscar_deals()
        activities = buscar_activities()
        deal_ids_validos, mapa_deal_owner = buscar_deals_rv()
        return jsonify({
            "colaborador": colaborador,
            "metas": metas,
            "ote": ote,
            "user_id": user_id,
            "total_deals": len(deals),
            "total_activities": len(activities),
            "total_deals_rv": len(deal_ids_validos),
        })
    except Exception as e:
        import traceback
        return jsonify({"erro": str(e), "trace": traceback.format_exc()})


@app.route("/api/closer")
def api_closer():
    if "nome" not in session:
        return jsonify({"erro": "Não autenticado"}), 401
    nome = session["nome"]
    try:
        colaborador = buscar_colaborador(nome)
        if not colaborador:
            return jsonify({"erro": f"'{nome}' nao encontrado"}), 404

        metas = buscar_metas(nome)
        if not metas:
            return jsonify({"erro": f"Metas nao encontradas para o mes atual"}), 404

        ote = buscar_ote(colaborador["cargo"])
        if not ote:
            return jsonify({"erro": f"OTE nao encontrado"}), 404

        users   = buscar_users()
        user_id = encontrar_user_id(users, nome)
        if not user_id:
            return jsonify({"erro": f"Usuario nao encontrado no Pipedrive"}), 404

        deals      = buscar_deals()
        activities = buscar_activities()
        referidos  = buscar_referidos()

        resultado = calcular_closer(nome, user_id, colaborador, metas, ote, deals, activities, referidos)
        return jsonify(limpar_nans(resultado))

    except Exception as e:
        import traceback
        return jsonify({"erro": str(e), "trace": traceback.format_exc()}), 500

@app.route("/api/tipo")
def api_tipo():
    """Retorna se o usuário logado é SDR ou Closer."""
    if "nome" not in session:
        return jsonify({"erro": "Não autenticado"}), 401
    nome = session["nome"]
    colaborador = buscar_colaborador(nome)
    if not colaborador:
        return jsonify({"tipo": "desconhecido"})
    tipo = "closer" if is_closer(colaborador["cargo"]) else "sdr"
    return jsonify({"tipo": tipo, "cargo": colaborador["cargo"]})

# ── MAIN ─────────────────────────────────────────────────────
if __name__ == "__main__":
    port = int(os.getenv("PORT", 5050))
    print(f"\n Board Academy — http://localhost:{port}")
    app.run(host="0.0.0.0", port=port, debug=False)
