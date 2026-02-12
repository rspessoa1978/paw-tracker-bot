import os
import json
import re
import pandas as pd
from pathlib import Path

from google import genai

# --- CONFIGURAÇÕES ---
FILE_NAME = "database.xlsx"
client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))

def setup_pybliometrics():
    """
    Inicializa o pybliometrics de forma não-interativa (ideal para CI),
    criando o arquivo pybliometrics.cfg no caminho definido.
    """
    scopus_key_env = os.getenv("SCOPUS_API_KEY", "").strip()
    if not scopus_key_env:
        raise RuntimeError("SCOPUS_API_KEY não encontrada nas variáveis de ambiente.")

    # Permite múltiplas chaves separadas por vírgula
    keys = [k.strip() for k in scopus_key_env.split(",") if k.strip()]

    # InstToken é opcional (útil fora da rede/VPN da instituição)
    inst_env = os.getenv("SCOPUS_INST_TOKEN", "").strip()
    inst_tokens = [t.strip() for t in inst_env.split(",") if t.strip()] if inst_env else None

    # Caminho explícito para evitar ambiguidades de HOME em CI
    # Você pode deixar o default (~/.config/pybliometrics.cfg) ou fixar no workspace.
    default_cfg = Path.home() / ".config" / "pybliometrics.cfg"
    config_path = Path(os.getenv("PYBLIOMETRICS_CONFIG_PATH", str(default_cfg)))
    config_path.parent.mkdir(parents=True, exist_ok=True)

    import pybliometrics
    pybliometrics.init(config_path=config_path, keys=keys, inst_tokens=inst_tokens)

    print(f"✓ pybliometrics inicializado. Config: {config_path}", flush=True)

def ask_gemini_classification(title, abstract):
    prompt = (
        f"Analise o artigo sobre PAW: {title}. "
        f"Abstract: {abstract or ''}. "
        "Retorne APENAS um JSON com as chaves: "
        "Domain, Reactor, Gas, Time, Power, pH, ORP, Cond, H2O2, NO2, NO3, Endpoint."
    )
    try:
        response = client.models.generate_content(model="gemini-2.0-flash", contents=prompt)

        # Extrai o primeiro objeto JSON do texto (mais robusto que replace de fences)
        m = re.search(r"\{.*\}", response.text, flags=re.DOTALL)
        if not m:
            raise ValueError(f"Resposta do Gemini não contém JSON válido. Texto: {response.text[:300]}")

        return json.loads(m.group(0))
    except Exception as e:
        print(f"Erro IA: {e}", flush=True)
        return None

def main():
    # 1) Inicializa pybliometrics ANTES de importar classes Scopus
    setup_pybliometrics()

    # 2) Import tardio (ok) depois do init
    from pybliometrics.scopus import ScopusSearch

    if not os.path.exists(FILE_NAME):
        print(f"Erro: O arquivo {FILE_NAME} não foi encontrado.", flush=True)
        return

    print("Lendo banco de dados...", flush=True)
    df = pd.read_excel(FILE_NAME)

    if "DOI_clean" in df.columns:
        existing_dois = set(df["DOI_clean"].dropna().astype(str).str.strip().unique())
    else:
        existing_dois = set()

    print("Iniciando busca no Scopus...", flush=True)

    query = 'TITLE-ABS-KEY("plasma-activated water" OR "plasma-activated liquids")'

    # Em CI, frequentemente você NÃO está na rede/VPN institucional.
    # subscriber=False reduz a chance de 401 (mas pode limitar campos retornados).
    search = ScopusSearch(query, refresh=True, subscriber=False)

    new_rows = []
    if getattr(search, "results", None):
        for res in search.results:
            doi = (res.doi or "").strip()
            if not doi or doi in existing_dois:
                continue

            title = (res.title or "").strip()
            abstract = (getattr(res, "description", None) or "").strip()

            print(f"Novo paper encontrado: {title}", flush=True)

            data = ask_gemini_classification(title, abstract)
            if not data:
                continue

            core6_fields = ["pH", "ORP", "Cond", "H2O2", "NO2", "NO3"]
            core6_count = sum(1 for k in core6_fields if data.get(k) is not None)

            year = None
            if getattr(res, "coverDate", None):
                year = str(res.coverDate).split("-")[0]

            new_rows.append({
                "PAW (cleaned)": "YES",
                "Year": year,
                "Title": title,
                "Authors": getattr(res, "author_names", None),
                "Source title": getattr(res, "publicationName", None),
                "DOI_clean": doi,
                "Abstract": abstract,
                "Domain (auto)": data.get("Domain"),
                "Core6_pH (auto)": data.get("pH"),
                "Core6_H2O2 (auto)": data.get("H2O2"),
                "Core6 count": core6_count,
                "Endpoint (auto)": data.get("Endpoint"),
            })

    if new_rows:
        updated_df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)
        updated_df.to_excel(FILE_NAME, index=False)
        print(f"Sucesso: {len(new_rows)} novos artigos adicionados.", flush=True)
    else:
        print("Nenhuma novidade encontrada no Scopus hoje.", flush=True)

if __name__ == "__main__":
    main()
