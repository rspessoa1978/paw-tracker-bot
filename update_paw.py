import os
import json
import pandas as pd
from pathlib import Path

# =================================================================
# 1. CONFIGURAÇÃO DO AMBIENTE (DEVE EXECUTAR ANTES DE QUALQUER IMPORT DO SCOPUS)
# =================================================================
def bootstrap_scopus():
    scopus_dir = Path.home() / ".scopus"
    scopus_dir.mkdir(exist_ok=True)
    config_file = scopus_dir / "config.ini"
    
    scopus_key = os.getenv("SCOPUS_API_KEY")
    if scopus_key:
        with open(config_file, "w") as f:
            f.write(f"[Authentication]\nAPIKey = {scopus_key}\n")
        print("✓ Configuração do Scopus criada com sucesso no disco.")
    else:
        print("! AVISO: SCOPUS_API_KEY não encontrada nas variáveis de ambiente.")

# Executa a criação do ficheiro imediatamente
bootstrap_scopus()

# =================================================================
# 2. AGORA SIM, IMPORTAR AS BIBLIOTECAS QUE DEPENDEM DO CONFIG.INI
# =================================================================
from pybliometrics.scopus import ScopusSearch
from google import genai

# --- CONFIGURAÇÕES DO PROJECTO ---
FILE_NAME = 'database.xlsx'
client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))

def ask_gemini_classification(title, abstract):
    prompt = f"""
    Analise o seguinte artigo sobre Plasma-Activated Water (PAW):
    Título: {title}
    Abstract: {abstract}

    Retorne APENAS um JSON (use 1 para presente e 0 para ausente):
    {{
        "Domain": "Agriculture, Food Systems, Biomedical, Fundamentals ou Environmental",
        "Reactor": "Tipo de reator",
        "Gas": "Gás usado",
        "Time": 1,
        "Power": 1,
        "pH": 1,
        "ORP": 1,
        "Cond": 1,
        "H2O2": 1,
        "NO2": 1,
        "NO3": 1,
        "Endpoint": "Alvo principal"
    }}
    """
    try:
        response = client.models.generate_content(model="gemini-2.0-flash", contents=prompt)
        clean_json = response.text.replace('```json', '').replace('```', '').strip()
        return json.loads(clean_json)
    except Exception as e:
        print(f"Erro na classificação IA: {e}")
        return None

def main():
    if not os.path.exists(FILE_NAME):
        print(f"Erro crítico: O ficheiro {FILE_NAME} não foi encontrado no repositório.")
        return

    print("A carregar base de dados...")
    df = pd.read_excel(FILE_NAME)
    
    if 'DOI_clean' not in df.columns:
        print("Erro: Coluna 'DOI_clean' não encontrada no Excel.")
        return

    existing_dois = set(df['DOI_clean'].dropna().astype(str).unique())

    print("A pesquisar novos artigos no Scopus...")
    query = 'TITLE-ABS-KEY("plasma-activated water" OR "plasma-activated liquids")'
    search = ScopusSearch(query, refresh=True)
    
    new_rows = []
    if search.results:
        for res in search.results:
            if res.doi not in existing_dois and res.doi:
                print(f"-> Analisando: {res.title}")
                data = ask_gemini_classification(res.title, res.description)
                
                if data:
                    new_rows.append({
                        'PAW (cleaned)': 'YES',
                        'Year': res.coverDate.split('-')[0] if res.coverDate else None,
                        'Title': res.title,
                        'Authors': res.author_names,
                        'Source title': res.publicationName,
                        'DOI_clean': res.doi,
                        'Abstract': res.description,
                        'Domain (auto)': data.get('Domain'),
                        'Reactor family (auto)': data.get('Reactor'),
                        'Working gas (auto)': data.get('Gas'),
                        'Treatment time reported (auto)': data.get('Time'),
                        'Power/energy reported (auto)': data.get('Power'),
                        'Core6_pH (auto)': data.get('pH'),
                        'Core6_ORP (auto)': data.get('ORP'),
                        'Core6_Conductivity/TDS (auto)': data.get('Cond'),
                        'Core6_H2O2 (auto)': data.get('H2O2'),
                        'Core6_NO2- (auto)': data.get('NO2'),
                        'Core6_NO3- (auto)': data.get('NO3'),
                        'Core6 count': sum([data.get(k, 0) for k in ['pH', 'ORP', 'Cond', 'H2O2', 'NO2', 'NO3']]),
                        'Endpoint (auto)': data.get('Endpoint')
                    })

    if new_rows:
        updated_df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)
        updated_df.to_excel(FILE_NAME, index=False)
        print(f"Sucesso: {len(new_rows)} novos artigos adicionados.")
    else:
        print("A planilha já está atualizada. Nada a acrescentar.")

if __name__ == "__main__":
    main()