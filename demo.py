"""Script de Teste / Demonstração Rápida.

Gera um arquivo de teste com 1 milhão de linhas e executa o pipeline
para demonstrar a lógica de fatiamento em chunks e escrita incremental.
"""

import os
import shutil
import time
import numpy as np
import pandas as pd
import pipeline


def gerar_dados_simulados(caminho_csv, total_linhas=1_000_000):
    """Gera uma base fictícia de logs/telemetria para teste local."""
    print(f"[*] Gerando base simulada de teste com {total_linhas:,} linhas...")
    os.makedirs(os.path.dirname(caminho_csv), exist_ok=True)

    dispositivos = [
        "dispositivo_01",
        "dispositivo_02",
        "dispositivo_03",
        "dispositivo_04",
        "dispositivo_05",
        "dispositivo_06",
        "dispositivo_07",
        "dispositivo_08",
    ]

    # Gera em lotes para não consumir muita memória durante o teste
    lote = 250_000
    geradas = 0
    primeiro = True

    while geradas < total_linhas:
        qtd_atual = min(lote, total_linhas - geradas)
        df_lote = pd.DataFrame(
            {
                "data_hora": pd.date_range("2026-01-01", periods=qtd_atual, freq="10ms"),
                "id_entidade": np.random.choice(dispositivos, size=qtd_atual),
                "uso_cpu": np.random.uniform(10.0, 95.0, size=qtd_atual).round(2),
                "latencia_ms": np.random.exponential(scale=20.0, size=qtd_atual).round(1),
                "codigo_status": np.random.choice([200, 201, 400, 500], size=qtd_atual),
            }
        )

        df_lote.to_csv(
            caminho_csv,
            mode="w" if primeiro else "a",
            header=primeiro,
            index=False,
            sep=";",
        )
        primeiro = False
        geradas += qtd_atual
        print(f"  -> {geradas:,} de {total_linhas:,} linhas criadas...")

    tamanho_mb = os.path.getsize(caminho_csv) / (1024 * 1024)
    print(f"[OK] Base de teste criada ({tamanho_mb:.2f} MB): {caminho_csv}\n")


def main():
    pasta_teste_entrada = "./dados_teste/origem"
    pasta_teste_saida = "./dados_teste/processados"
    arquivo_teste = os.path.join(pasta_teste_entrada, "telemetria_exemplo.csv")

    # Limpa execuções antigas de teste se existirem
    if os.path.exists("./dados_teste"):
        shutil.rmtree("./dados_teste")

    # 1. Gera base de teste
    gerar_dados_simulados(arquivo_teste, total_linhas=1_000_000)

    # 2. Configura as variáveis do pipeline para o ambiente de teste
    pipeline.PASTA_ENTRADA = pasta_teste_entrada
    pipeline.PASTA_SAIDA = pasta_teste_saida
    pipeline.TAMANHO_CHUNK = 250_000  # 250k linhas por chunk no teste

    # 3. Executa o pipeline
    print("Iniciando a execucao do pipeline...")
    t_inicio = time.time()
    pipeline.executar_pipeline()
    t_fim = time.time()

    # 4. Exibe os resultados gerados em disco
    print("\nArquivos gerados nas pastas de saida:")
    for pasta_raiz, _, arquivos in os.walk(pasta_teste_saida):
        for arq in arquivos:
            caminho = os.path.join(pasta_raiz, arq)
            qtd_linhas = sum(1 for _ in open(caminho, encoding="utf-8")) - 1
            print(f"  - {caminho} -> {qtd_linhas:,} linhas")


if __name__ == "__main__":
    main()
