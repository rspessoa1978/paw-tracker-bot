import os
import json
import pandas as pd
from pathlib import Path

# 1. FUNÇÃO DE CONFIGURAÇÃO (Cria o arquivo antes de qualquer coisa)
def setup_scopus_file():
    scopus_dir = Path.home() / ".scopus"
    scopus_dir.mkdir(exist_ok=True)
    config_file = scopus_dir / "config.ini"
    
    scopus_key = os.getenv("SCOPUS_API_KEY")
    if scopus_key:
        with open(config_file, "w") as f:
            f.write(f"[Authentication]\nAPIKey = {scopus_key}\n")
        print("✓ Arquivo config.ini criado com sucesso no diretório Home.")
    else:
        print("! Erro: SCOPUS_API_KEY não encontrada.")

# --- Executa a configuração imediatamente ao abrir o script ---
setup_scopus_file()

# Importamos o Gemini aqui, que não depende do arquivo do Scopus
from google import genai

# --- CONFIGURAÇÕES ---
FILE_NAME = 'database.xlsx'
client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))

def ask_gemini_classification(title, abstract):
    prompt = f"Analise o artigo sobre PAW: {title}. Abstract: {abstract}. Retorne um JSON com Domain, Reactor, Gas, Time, Power, pH, ORP, Cond, H2O2, NO2, NO3, Endpoint."
    try:
        response = client.models.generate_content(model="gemini-2.0-flash", contents=prompt)
        clean_json = response.text.replace('```json', '').replace('```', '').strip()
        return json.loads(clean_json)
    except Exception as e:
        print(f"Erro IA: {e}")
        return None

def main():
    # 2. IMPORTAÇÃO TARDIA (AQUI ESTÁ O SEGREDO)
    # Só importamos o ScopusSearch aqui dentro, garantindo que o arquivo já existe.
    from pybliometrics.scopus import ScopusSearch
    
    if not os.path.exists(FILE_NAME):
        print(f"Erro: {FILE_NAME} não encontrado.")
        return

    print("Lendo banco de dados...")
    df = pd.read_excel(FILE_NAME)
    existing_dois = set(df['DOI_clean'].dropna().astype(str).unique())

    print("Iniciando busca no Scopus...")
    query = 'TITLE-ABS-KEY("plasma-activated water" OR "plasma-activated liquids")'
    search = ScopusSearch(query, refresh=True)
    
    new_rows = []
    if search.results:
        for res in search.results:
            if res.doi not in existing_dois and res.doi:
                print(f"Novo: {res.title}")
                data = ask_gemini_classification(res.title, res.description)
                if data:
                    new_rows.append({
                        'PAW (cleaned)': 'YES',
                        'Year': res.coverDate.split('-')[0] if res.coverDate else None,
                        'Title': res.title,
                        'Authors': res.author_names,
                        'DOI_clean': res.doi,
                        'Abstract': res.description,
                        'Domain (auto)': data.get('Domain'),
                        'Core6_pH (auto)': data.get('pH'),
                        'Core6_H2O2 (auto)': data.get('H2O2'),
                        'Core6 count': sum([data.get(k, 0) for k in ['pH', 'ORP', 'Cond', 'H2O2', 'NO2', 'NO3']]),
                        'Endpoint (auto)': data.get('Endpoint')
                    })

    if new_rows:
        updated_df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)
        updated_df.to_excel(FILE_NAME, index=False)
        print(f"Sucesso! {len(new_rows)} novos artigos adicionados.")
    else:
        print("Nenhuma novidade encontrada.")

if __name__ == "__main__":
    main()