#!/usr/bin/env python3
"""
Baixa os Boletins de Urna (BU) 2026 - 1º e 2º turnos, por UF,
do portal de dados abertos do TSE, e salva na pasta de destino.

Uso (Windows, PowerShell ou CMD):
    python baixar_bu.py
    python baixar_bu.py --destino "D:\\git\\eleicoes2026\\bu"
    python baixar_bu.py --verificar     # também confere o SHA-512 de cada zip

Características:
- Pula arquivos já baixados completos (compara com o tamanho do servidor).
- Retoma downloads interrompidos (HTTP Range).
- Grava em arquivo temporário (.part) e só renomeia ao final.
- Com --verificar, baixa o .sha512 correspondente e confere a integridade.

Requer apenas Python 3 (sem dependências externas).
"""

import argparse
import hashlib
import os
import sys
import time
import urllib.request
import urllib.error

BASE = "https://cdn.tse.jus.br/estatistica/sead/eleicoes/eleicoes2026/buweb/"
DATA_1T = "051020261403"   # timestamp dos arquivos do 1º turno (conforme o portal)
DATA_2T = "261020260742"   # timestamp dos arquivos do 2º turno (conforme o portal)

UFS = ["AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS",
       "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC",
       "SE", "SP", "TO", "ZZ"]

# RR não tem arquivo do 2º turno no portal (listado só o 1º turno).
SEM_2T = {"RR"}


def montar_lista():
    arquivos = []
    for uf in UFS:
        arquivos.append(f"bweb_1t_{uf}_{DATA_1T}.zip")
        if uf not in SEM_2T:
            arquivos.append(f"bweb_2t_{uf}_{DATA_2T}.zip")
    return arquivos  # 28 + 27 = 55 arquivos


def tamanho_remoto(url):
    req = urllib.request.Request(url, method="HEAD", headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=60) as r:
        tam = r.headers.get("Content-Length")
        return int(tam) if tam else None


def baixar(url, destino, tentativas=5):
    parte = destino + ".part"
    for t in range(1, tentativas + 1):
        try:
            existente = os.path.getsize(parte) if os.path.exists(parte) else 0
            headers = {"User-Agent": "Mozilla/5.0"}
            if existente:
                headers["Range"] = f"bytes={existente}-"
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=120) as r:
                modo = "ab" if existente and r.status == 206 else "wb"
                if modo == "wb":
                    existente = 0
                with open(parte, modo) as f:
                    while True:
                        bloco = r.read(1024 * 1024)
                        if not bloco:
                            break
                        f.write(bloco)
            os.replace(parte, destino)
            return True
        except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
            print(f"    tentativa {t}/{tentativas} falhou: {e}")
            time.sleep(min(30, 2 ** t))
    return False


def baixar_texto(url):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return r.read().decode("utf-8", errors="replace")


def sha512_arquivo(caminho):
    h = hashlib.sha512()
    with open(caminho, "rb") as f:
        for bloco in iter(lambda: f.read(1024 * 1024), b""):
            h.update(bloco)
    return h.hexdigest()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--destino", default=r"D:\git\eleicoes2026\bu")
    ap.add_argument("--verificar", action="store_true", help="confere SHA-512 de cada zip")
    args = ap.parse_args()

    os.makedirs(args.destino, exist_ok=True)
    arquivos = montar_lista()
    print(f"{len(arquivos)} arquivos -> {args.destino}\n")

    ok, pulados, falhas, invalidos = 0, 0, [], []
    for i, nome in enumerate(arquivos, 1):
        url = BASE + nome
        destino = os.path.join(args.destino, nome)
        print(f"[{i:02d}/{len(arquivos)}] {nome}")
        try:
            tam_remoto = tamanho_remoto(url)
        except Exception as e:
            print(f"    não consegui consultar o tamanho: {e}")
            tam_remoto = None

        if os.path.exists(destino) and tam_remoto and os.path.getsize(destino) == tam_remoto:
            print("    já existe e está completo — pulando")
            pulados += 1
        else:
            if tam_remoto:
                print(f"    tamanho: {tam_remoto / 1024 / 1024:.1f} MB")
            if baixar(url, destino):
                print("    OK")
                ok += 1
            else:
                print("    FALHOU")
                falhas.append(nome)
                continue

        if args.verificar:
            try:
                esperado = baixar_texto(url + ".sha512").split()[0].lower()
                obtido = sha512_arquivo(destino)
                if esperado == obtido:
                    print("    SHA-512 confere")
                else:
                    print("    SHA-512 NÃO confere")
                    invalidos.append(nome)
            except Exception as e:
                print(f"    não consegui verificar o SHA-512: {e}")

    print("\n--- Resumo ---")
    print(f"baixados agora: {ok} | já existiam: {pulados} | falhas: {len(falhas)}")
    if falhas:
        print("Falharam (rode o script de novo para tentar outra vez):")
        for n in falhas:
            print("  -", n)
    if invalidos:
        print("SHA-512 inválido (baixe de novo):")
        for n in invalidos:
            print("  -", n)
    return 1 if falhas or invalidos else 0


if __name__ == "__main__":
    sys.exit(main())
