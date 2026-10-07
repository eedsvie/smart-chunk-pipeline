"""Pipeline de Processamento em Lotes (Chunks) - Big Data Single-Node.

Projeto Aplicado: MBA em Big Data e Inteligência Competitiva
Disciplina: Ferramentas para Big Data

Objetivo:
Processar bases volumosas (500M+ linhas) em uma única máquina,
evitando estouro de memória (MemoryError) através do paradigma
'Dividir para Conquistar'.
"""

import os
from os.path import exists, join
import time
import pandas as pd

# ==============================================================================
# CONFIGURAÇÕES DO PIPELINE
# ==============================================================================
PASTA_ENTRADA = "./dados/origem_visoes"
PASTA_SAIDA = "./dados/saida_processada"
TAMANHO_CHUNK = 5_000_000  # 5 milhões de linhas por iteração em memória
DELIMITADOR = ";"

# Mapeamento de regras de agrupamento (anonimizado)
# Exemplo: Agrupando registros por região / tipo de nó / dispositivo
GRUPOS_REGRAS = {
    "grupo_alpha": ["dispositivo_01", "dispositivo_02", "dispositivo_03"],
    "grupo_beta": ["dispositivo_04", "dispositivo_05"],
    "grupo_gamma": ["dispositivo_06", "dispositivo_07", "dispositivo_08"],
}


# ==============================================================================
# FUNÇÕES DE PERSISTÊNCIA E PROCESSAMENTO
# ==============================================================================
def salvar_chunk_filtrado(df_filtrado, nome_grupo, nome_arquivo, modo="w", incluir_cabecalho=True):
    """Salva o subconjunto de dados filtrado na pasta de destino correspondente.

    Se for a primeira vez que o arquivo é gravado, utiliza mode='w' com cabeçalho.
    Nas iterações seguintes, utiliza mode='a' (append) sem cabeçalho para não duplicar.
    """
    pasta_destino = join(PASTA_SAIDA, f"resultado_{nome_grupo}")
    os.makedirs(pasta_destino, exist_ok=True)

    caminho_completo = join(pasta_destino, nome_arquivo)

    # Remove colunas auxiliares de processamento, se houver
    if "coluna_auxiliar" in df_filtrado.columns:
        df_filtrado = df_filtrado.drop(columns=["coluna_auxiliar"])

    df_filtrado.to_csv(
        caminho_completo,
        index=False,
        sep=DELIMITADOR,
        mode=modo,
        header=incluir_cabecalho,
    )


def processar_chunk_por_grupos(chunk_df, nome_arquivo, arquivos_rastreados):
    """Aplica o filtro para cada grupo de interesse no chunk atual

    e envia para a gravação incremental.
    """
    # Exemplo de tratamento ou enriquecimento simples de coluna se necessário
    if "id_entidade" not in chunk_df.columns and "fdn" in chunk_df.columns:
        chunk_df["id_entidade"] = chunk_df["fdn"].str.split("/").str[-1]

    for chave_grupo, lista_entidades in GRUPOS_REGRAS.items():
        # Filtra apenas os registros pertencentes a este grupo
        filtro = chunk_df["id_entidade"].isin(lista_entidades)
        df_grupo = chunk_df[filtro]

        if len(df_grupo) > 0:
            caminho_destino = join(PASTA_SAIDA, f"resultado_{chave_grupo}", nome_arquivo)

            # Lógica de controle de cabeçalho dinâmico
            if caminho_destino in arquivos_rastreados:
                modo = "a"  # Já foi criado antes, apenas adiciona novas linhas
                cabecalho = False
            else:
                modo = "w"  # Primeiro chunk do arquivo: cria e escreve o cabeçalho
                cabecalho = True
                arquivos_rastreados.add(caminho_destino)

            salvar_chunk_filtrado(
                df_filtrado=df_grupo,
                nome_grupo=chave_grupo,
                nome_arquivo=nome_arquivo,
                modo=modo,
                incluir_cabecalho=cabecalho,
            )


def executar_pipeline():
    """Percorre a pasta de entrada e processa todos os arquivos CSV em chunks,

    mantendo o consumo de memória RAM baixo e constante.
    """
    if not exists(PASTA_ENTRADA):
        print(f"[!] Pasta de entrada '{PASTA_ENTRADA}' não encontrada. Crie a pasta ou configure o caminho.")
        return

    arquivos = [f for f in os.listdir(PASTA_ENTRADA) if f.endswith(".csv")]
    total_arquivos = len(arquivos)

    if total_arquivos == 0:
        print(f"[!] Nenhum arquivo CSV encontrado em '{PASTA_ENTRADA}'.")
        return

    print(f"[*] Iniciando processamento de {total_arquivos} arquivo(s)...")
    tempo_inicio = time.time()
    total_linhas_processadas = 0

    for idx, nome_arquivo in enumerate(arquivos, start=1):
        caminho_arquivo = join(PASTA_ENTRADA, nome_arquivo)
        print(f"\n--> Processando arquivo [{idx}/{total_arquivos}]: {nome_arquivo}")

        # Set para rastrear os arquivos de saída já criados para este arquivo de entrada
        arquivos_rastreados = set()
        linhas_arquivo = 0

        # Leitura em streaming (chunks) para não estourar a memória RAM
        leitor = pd.read_csv(
            caminho_arquivo,
            sep=DELIMITADOR,
            low_memory=False,
            on_bad_lines="skip",
            chunksize=TAMANHO_CHUNK,
        )

        num_chunk = 1
        for chunk in leitor:
            linhas_chunk = len(chunk)
            linhas_arquivo += linhas_chunk
            processar_chunk_por_grupos(chunk, nome_arquivo, arquivos_rastreados)
            print(f"    - Chunk #{num_chunk} concluído ({linhas_chunk:,} linhas)")
            num_chunk += 1

        total_linhas_processadas += linhas_arquivo
        print(f"[OK] {nome_arquivo} finalizado. Total: {linhas_arquivo:,} linhas.")

    tempo_total = time.time() - tempo_inicio
    print("\n========================================================")
    print(f"Pipeline finalizado com sucesso!")
    print(f"Total de registros processados: {total_linhas_processadas:,}")
    print(f"Tempo total de execução: {tempo_total:.2f} segundos")
    print("========================================================")


if __name__ == "__main__":
    executar_pipeline()
