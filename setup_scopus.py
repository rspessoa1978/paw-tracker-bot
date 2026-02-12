# setup_scopus.py
import os
from pathlib import Path
import pybliometrics

def main():
    # 1) Lê a(s) chave(s)
    raw_keys = (os.getenv("SCOPUS_API_KEY") or "").strip()
    if not raw_keys:
        raise RuntimeError("SCOPUS_API_KEY não definida nas variáveis de ambiente.")

    keys = [k.strip() for k in raw_keys.split(",") if k.strip()]

    # 2) InstToken é opcional (acesso fora da rede/VPN da instituição)
    raw_tokens = (os.getenv("SCOPUS_INST_TOKEN") or "").strip()
    inst_tokens = [t.strip() for t in raw_tokens.split(",") if t.strip()] if raw_tokens else None

    # 3) Caminho explícito do config (evita ambiguidade no runner)
    config_path = Path.home() / ".config" / "pybliometrics.cfg"
    config_path.parent.mkdir(parents=True, exist_ok=True)

    # 4) (Opcional, mas recomendável) remove configs legadas que podem ter precedência
    legacy1 = Path.home() / ".scopus" / "config.ini"
    legacy2 = Path.home() / ".pybliometrics" / "config.ini"
    for p in (legacy1, legacy2):
        try:
            if p.exists():
                p.unlink()
        except Exception:
            pass

    # 5) Inicializa/cria o pybliometrics.cfg de forma não interativa
    pybliometrics.init(config_path=config_path, keys=keys, inst_tokens=inst_tokens)

    print(f"✓ Configuração criada/atualizada em: {config_path}")

if __name__ == "__main__":
    main()
