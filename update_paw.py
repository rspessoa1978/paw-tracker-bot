import os
import pandas as pd
from pathlib import Path
from datetime import date, timedelta, datetime
from typing import Optional

try:
    from zoneinfo import ZoneInfo
    TZ = ZoneInfo("America/Sao_Paulo")
except Exception:
    TZ = None  # fallback seguro

# =====================
# CONFIGURAÇÕES
# =====================
FILE_NAME = Path(__file__).with_name("database.xlsx")

BOT_DATE_COL = "Added_to_db"     # coluna de controle (data que o bot adicionou)
OVERLAP_DAYS = 2                 # sobreposição para não perder nada por fuso/atrasos

SEARCH_TERMS = (
    'TITLE-ABS-KEY('
    '"plasma-activated water" OR "plasma activated water" OR '
    '"plasma-activated liquid*" OR "plasma activated liquid*" OR '
    '"plasma-activated liquids" OR "plasma activated liquids"'
    ')'
)

def yyyymmdd(d: date) -> str:
    return d.strftime("%Y%m%d")

def init_pybliometrics() -> None:
    """
    Inicializa o pybliometrics em CI sem prompt e com caminho explícito, se definido.
    """
    raw_keys = (os.getenv("SCOPUS_API_KEY") or "").strip()
    if not raw_keys:
        raise RuntimeError("SCOPUS_API_KEY não encontrada nas variáveis de ambiente.")

    keys = [k.strip() for k in raw_keys.split(",") if k.strip()]

    raw_tokens = (os.getenv("SCOPUS_INST_TOKEN") or "").strip()
    inst_tokens = [t.strip() for t in raw_tokens.split(",") if t.strip()] if raw_tokens else None

    cfg = os.getenv("PYBLIOMETRICS_CONFIG_PATH")
    if cfg:
        cfg_path = Path(cfg)
    else:
        cfg_path = Path.home() / ".config" / "pybliometrics.cfg"

    cfg_path.parent.mkdir(parents=True, exist_ok=True)

    import pybliometrics
    pybliometrics.init(config_path=cfg_path, keys=keys, inst_tokens=inst_tokens)

    print(f"✓ pybliometrics inicializado. Config: {cfg_path}", flush=True)

def load_db() -> pd.DataFrame:
    if not FILE_NAME.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {FILE_NAME}")

    df = pd.read_excel(FILE_NAME)

    # Garante colunas essenciais
    if "EID" not in df.columns:
        df["EID"] = pd.NA
    if "DOI_clean" not in df.columns:
        df["DOI_clean"] = pd.NA
    if BOT_DATE_COL not in df.columns:
        df[BOT_DATE_COL] = pd.NaT

    return df

def get_last_added_date(df: pd.DataFrame) -> Optional[date]:
    s = pd.to_datetime(df[BOT_DATE_COL], errors="coerce")
    if s.notna().any():
        return s.max().date()
    return None

def build_query(day_start: date, day_end: date) -> str:
    # Janela [AFT day_start, BEF day_end)
    return (
        f"{SEARCH_TERMS} "
        f"AND ORIG-LOAD-DATE AFT {yyyymmdd(day_start)} "
        f"AND ORIG-LOAD-DATE BEF {yyyymmdd(day_end)}"
    )

def daterange(d0: date, d1: date):
    d = d0
    while d < d1:
        yield d
        d += timedelta(days=1)

def scopus_daily_search(start_date: date, end_date: date):
    """
    Busca no Scopus dia-a-dia para evitar exceder limite de 5000 resultados por query.
    """
    from pybliometrics.scopus import ScopusSearch
    from pybliometrics.exception import Scopus400Error, ScopusQueryError

    all_results = []
    for d in daterange(start_date, end_date):
        q = build_query(d, d + timedelta(days=1))
        try:
            s = ScopusSearch(q, refresh=True, subscriber=False)
        except (Scopus400Error, ScopusQueryError) as e:
            raise RuntimeError(
                f"Falha na busca para {d.isoformat()} (janela diária). "
                f"Erro: {type(e).__name__}: {e}"
            ) from e

        if getattr(s, "results", None):
            all_results.extend(s.results)

    return all_results

def append_new_rows(df: pd.DataFrame, results, today: date) -> pd.DataFrame:
    # Conjuntos existentes (limpando NaN)
    existing_eids = set(
        df["EID"].dropna().astype(str).str.strip()
    )
    existing_dois = set(
        df["DOI_clean"].dropna().astype(str).str.strip()
    )

    new_rows = []

    for r in results:
        eid = (getattr(r, "eid", None) or "").strip()
        if not eid or eid in existing_eids:
            continue

        doi = (getattr(r, "doi", None) or "").strip()
        if doi and doi in existing_dois:
            continue

        cover = getattr(r, "coverDate", None)
        year = cover.split("-")[0] if cover else None

        # Monta linha preservando as colunas existentes
        row = {c: None for c in df.columns}

        row["PAW (cleaned)"] = "YES"
        row["Screening status"] = "NEW"
        row["Year"] = year
        row["Title"] = getattr(r, "title", None)
        row["Authors"] = getattr(r, "author_names", None)
        row["Source title"] = getattr(r, "publicationName", None)
        row["Document Type"] = getattr(r, "subtypeDescription", None)
        row["Cited by"] = getattr(r, "citedby_count", None)
        row["DOI_clean"] = doi if doi else None
        row["Link"] = getattr(r, "scopus_url", None)
        row["Abstract"] = getattr(r, "description", None)
        row["Author Keywords"] = getattr(r, "authkeywords", None)
        row["EID"] = eid
        row[BOT_DATE_COL] = pd.Timestamp(today)

        new_rows.append(row)

    if not new_rows:
        return df

    out = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)

    # Marca duplicidade de DOI, se a coluna existir
    if "Duplicate DOI" in out.columns and "DOI_clean" in out.columns:
        doi_series = out["DOI_clean"].astype("string")
        mask_valid = doi_series.notna() & (doi_series.str.len() > 0)
        dup = doi_series.duplicated(keep=False) & mask_valid
        out.loc[dup, "Duplicate DOI"] = "YES"

    # Ordena por Added_to_db desc (deixa “primeira linha” mais recente)
    out[BOT_DATE_COL] = pd.to_datetime(out[BOT_DATE_COL], errors="coerce")
    if "Year" in out.columns:
        out = out.sort_values(by=[BOT_DATE_COL, "Year"], ascending=[False, False], na_position="last")
    else:
        out = out.sort_values(by=[BOT_DATE_COL], ascending=[False], na_position="last")

    return out

def save_db(df: pd.DataFrame) -> None:
    df.to_excel(FILE_NAME, index=False)

def main():
    init_pybliometrics()

    df = load_db()

    # Data “hoje” em São Paulo (quando possível)
    if TZ is not None:
        today = datetime.now(TZ).date()
    else:
        today = date.today()

    last = get_last_added_date(df)

    # Se primeira execução, busca só ontem+hoje (conservador)
    if last is None:
        start = today - timedelta(days=1)
    else:
        start = last - timedelta(days=OVERLAP_DAYS)

    end = today + timedelta(days=1)  # inclui hoje
    print(f"Atualização incremental: {start.isoformat()} até {today.isoformat()} (ORIG-LOAD-DATE).", flush=True)

    results = scopus_daily_search(start, end)
    print(f"Resultados Scopus (janela incremental): {len(results)}", flush=True)

    updated = append_new_rows(df, results, today)
    added = len(updated) - len(df)
    save_db(updated)

    print(f"Concluído. Novas linhas adicionadas: {added}", flush=True)

if __name__ == "__main__":
    main()
