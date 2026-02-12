import os
import json
import pandas as pd
import google.generativeai as genai
from pybliometrics.scopus import ScopusSearch
from datetime import datetime

# --- CONFIGURAÇÃO DAS CHAVES ---
# No GitHub Actions, estas chaves serão lidas dos 'Secrets'
SCOPUS_API_KEY = os.getenv("SCOPUS_API_KEY")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

# Configurar APIs
os.environ['SCOPUS_API_KEY'] = SCOPUS_API_KEY
genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel('gemini-pro')

# Nome do arquivo original
FILE_NAME = 'scopus1_organized_PAW_coding_UPDATED_v3_numeric_core6.csv'

def ask_gemini_classification(title, abstract):
    """Envia o paper para o Gemini classificar conforme as colunas da planilha."""
    prompt = f"""
    Analise o seguinte artigo científico sobre Plasma-Activated Water (PAW):
    Título: {title}
    Abstract: {abstract}

    Classifique o artigo estritamente no formato JSON abaixo:
    {{
        "Domain": "Escolha entre: Agriculture, Food Systems, Biomedical, Fundamentals, ou Environmental",
        "Reactor_family": "Ex: DBD, Atmospheric Pressure Plasma Jet, Gliding Arc, etc.",
        "Working_gas": "Ex: Air, Argon, Oxygen, Nitrogen",
        "Time_reported": 1 ou 0,
        "Power_reported": 1 ou 0,
        "pH": 1 ou 0,
        "ORP": 1 ou 0,
        "Cond": 1 ou 0,
        "H2O2": 1 ou 0,
        "NO2": 1 ou 0,
        "NO3": 1 ou 0,
        "Endpoint": "Ex: Microbial inactivation, Seed germination, Cancer cell apoptosis, etc."
    }}
    Retorne APENAS o JSON, sem texto adicional.
    """
    try:
        response = model.generate_content(prompt)
        # Limpa a resposta para garantir que seja um JSON válido
        json_str = response.text.replace('```json', '').replace('```', '').strip()
        return json.loads(json_str)
    except Exception as e:
        print(f"Erro na IA: {e}")
        return None

def main():
    # 1. Carregar base atual
    df = pd.read_csv(FILE_NAME)
    existing_dois = set(df['DOI_clean'].dropna().unique())

    # 2. Buscar novos papers no Scopus
    # Busca por papers carregados recentemente (ex: nos últimos 30 dias)
    query = 'TITLE-ABS-KEY("plasma-activated water" OR "plasma-activated liquids")'
    print("Consultando Scopus...")
    search = ScopusSearch(query, refresh=True)
    
    new_records = []
    
    if search.results:
        for res in search.results:
            if res.doi not in existing_dois and res.doi is not None:
                print(f"Novo paper encontrado: {res.title}")
                
                # 3. Chamar Gemini para classificação
                classification = ask_gemini_classification(res.title, res.description)
                
                if classification:
                    # Montar nova linha seguindo as colunas da sua planilha
                    new_row = {
                        'PAW (cleaned)': 'YES',
                        'Year': res.coverDate.split('-')[0] if res.coverDate else None,
                        'Title': res.title,
                        'Authors': res.author_names,
                        'Source title': res.publicationName,
                        'Document Type': res.aggregationType,
                        'Cited by': res.citedby_count,
                        'DOI_clean': res.doi,
                        'Link': f"https://doi.org/{res.doi}",
                        'Abstract': res.description,
                        'Domain (auto)': classification.get('Domain'),
                        'Reactor family (auto)': classification.get('Reactor_family'),
                        'Working gas (auto)': classification.get('Working_gas'),
                        'Treatment time reported (auto)': classification.get('Time_reported'),
                        'Power/energy reported (auto)': classification.get('Power_reported'),
                        'Core6_pH (auto)': classification.get('pH'),
                        'Core6_ORP (auto)': classification.get('ORP'),
                        'Core6_Conductivity/TDS (auto)': classification.get('Cond'),
                        'Core6_H2O2 (auto)': classification.get('H2O2'),
                        'Core6_NO2- (auto)': classification.get('NO2'),
                        'Core6_NO3- (auto)': classification.get('NO3'),
                        'Endpoint (auto)': classification.get('Endpoint'),
                        'Core6 count': sum([classification.get(k, 0) for k in ['pH', 'ORP', 'Cond', 'H2O2', 'NO2', 'NO3']])
                    }
                    new_records.append(new_row)

    # 4. Salvar resultados
    if new_records:
        new_df = pd.DataFrame(new_records)
        updated_df = pd.concat([df, new_df], ignore_index=True)
        updated_df.to_csv(FILE_NAME, index=False)
        print(f"Sucesso! {len(new_records)} novos papers adicionados.")
    else:
        print("Nenhum paper novo encontrado hoje.")

if __name__ == "__main__":
    main()