import os
import pandas as pd
from pathlib import Path
from datetime import date, timedelta
from zoneinfo import ZoneInfo

# =====================
# CONFIGURAÇÕES
# =====================
TZ = ZoneInfo("America/Sao_Paulo")
FILE_NAME = Path(__file__).with_name("database.xlsx")

BOT_DATE_COL = "Added_to_db"     # coluna de controle (data que o bot adicionou)
OVERLAP_DAYS = 2                 # sobreposição para não perder nada por fuso/atrasos
MAX_FILL_PER_RUN = 0             # 0 = desliga "completar lacunas" (opcional)

SEARCH_TERMS = (
    'TITLE-ABS-KEY('
    '"plasma-activated water" OR "plasma activated water" OR '
    '"plasma-activated liquid*" OR "plasma activated liquid*" OR '
    '"plasma-activated liquids" OR "plasma activated liquids"'
    ')'
)

def yyyymmdd(d: date) -> str:
    return d.strftime("%Y%m%d")

def init_pybliometrics():
    """
    Inicializa pybliometrics criando/atualizando a config automaticamente.
    Evita o erro 'Please initialize Pybliometrics with init()'.
    """
    api_key = os.getenv("SCOPUS_API_KEY")
    if not api_key:
        raise RuntimeError("SCOPUS_API_KEY não encontrada nas variáveis de ambiente.")
    import pybliometrics.scopus
    # Cria config em ~/.config/pybliometrics.cfg e injeta a(s) chave(s)
    pybliometrics.scopus.init(keys=[api_key])

def load_db() -> pd.DataFrame:
    if not FILE_NAME.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {FILE_NAME}")
    df = pd.read_excel(FILE_NAME)

    # Garante a coluna de controle
    if BOT_DATE_COL not in df.columns:
        df[BOT_DATE_COL] = pd.NaT

    return df

def get_last_added_date(df: pd.DataFrame) -> date | None:
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
    """Itera d0, d0+1, ..., d1-1"""
    d = d0
    while d < d1:
        yield d
        d += timedelta(days=1)

def scopus_daily_search(start_date: date, end_date: date):
    """
    Busca no Scopus dia-a-dia para evitar exceder limite de 5000 resultados por query.
    """
    from pybliometrics.scopus import ScopusSearch
    from pybliometrics.scopus.exception import Scopus400Error

    all_results = []
    for d in daterange(start_date, end_date):
        q = build_query(d, d + timedelta(days=1))
        try:
            s = ScopusSearch(q, refresh=True, subscriber=False)
        except Scopus400Error as e:
            # Se isso acontecer num dia, a janela ainda está grande para seu nível de acesso.
            # Para PAW é improvável, mas deixamos uma mensagem clara.
            raise RuntimeError(
                f"Consulta diária ainda excedeu o limite em {d.isoformat()}. "
                f"Tente restringir mais o SEARCH_TERMS ou adicionar PUBYEAR."
            ) from e

        if s.results:
            all_results.extend(s.results)

    return all_results

def append_new_rows(df: pd.DataFrame, results, today: date) -> pd.DataFrame:
    # EID é sempre o identificador mais confiável (no seu arquivo, EID está completo).
    existing_eids = set(df["EID"].astype(str).str.strip())

    # DOI ajuda, mas pode faltar; então usamos EID como chave principal.
    existing_dois = set(
        df["DOI_clean"].dropna().astype(str).str.strip()
    ) if "DOI_clean" in df.columns else set()

    new_rows = []
    for r in results:
        eid = (getattr(r, "eid", None) or "").strip()
        if not eid:
            continue

        if eid in existing_eids:
            continue

        doi = (getattr(r, "doi", None) or "").strip()
        if doi and doi in existing_dois:
            # Já existe por DOI (caso raro se EID mudou/duplicou)
            continue

        cover = getattr(r, "coverDate", None)
        year = cover.split("-")[0] if cover else None

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
        row[BOT_DATE_COL] = today.isoformat()

        new_rows.append(row)

    if not new_rows:
        return df

    out = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)

    # Marca duplicidade de DOI (se a coluna existir)
    if "Duplicate DOI" in out.columns and "DOI_clean" in out.columns:
        dois = out["DOI_clean"].astype(str)
        mask_valid = out["DOI_clean"].notna()
        dup = dois.duplicated(keep=False) & mask_valid
        out.loc[dup, "Duplicate DOI"] = "YES"

    # Ordena para a “primeira linha” ser sempre a mais recente do bot
    out[BOT_DATE_COL] = pd.to_datetime(out[BOT_DATE_COL], errors="coerce")
    out = out.sort_values(by=[BOT_DATE_COL, "Year"], ascending=[False, False], na_position="last")

    return out

def save_db(df: pd.DataFrame):
    df.to_excel(FILE_NAME, index=False)

def main():
    init_pybliometrics()

    df = load_db()
    today = date.today() if TZ is None else date.fromtimestamp(__import__("time").time())

    # Melhor: usar timezone do Brasil
    today = __import__("datetime").datetime.now(TZ).date()

    last = get_last_added_date(df)

    # Se for a primeira execução (sem Added_to_db preenchido), comece pequeno para não estourar limite.
    if last is None:
        start = today - timedelta(days=1)
    else:
        start = last - timedelta(days=OVERLAP_DAYS)

    end = today + timedelta(days=1)  # inclui "hoje"
    print(f"Atualização incremental: {start.isoformat()} até {today.isoformat()} (ORIG-LOAD-DATE).")

    results = scopus_daily_search(start, end)
    print(f"Resultados Scopus (janela incremental): {len(results)}")

    updated = append_new_rows(df, results, today)
    added = len(updated) - len(df)
    save_db(updated)

    print(f"Concluído. Novas linhas adicionadas: {added}")

if __name__ == "__main__":
    main()
