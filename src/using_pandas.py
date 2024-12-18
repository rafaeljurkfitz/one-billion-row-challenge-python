import pandas as pd
from multiprocessing import Pool
from tqdm import tqdm  # importa o tqdm para barra de progresso
from dotenv import load_dotenv
import os

# Load .env file
load_dotenv()

DATA_PATH = str(os.getenv("DATA_PATH")) # Certifique-se de que este é o caminho correto para o arquivo
TOTAL_LINHAS = int(os.getenv("TOTAL_LINHAS")) #1_000_000_000  # Total de linhas conhecido
CHUNKSIZE = int(os.getenv("CHUNKSIZE")) #100_000_000  # Define o tamanho do chunk
CONCURRENCY = int(os.getenv("CONCURRENCY"))


def process_chunk(chunk):
    # Agrega os dados dentro do chunk usando Pandas
    if chunk.empty:
        return pd.DataFrame()
    return chunk.groupby('station')['measure'].agg(['min', 'max', 'mean']).reset_index()

def create_df_with_pandas(DATA_PATH, TOTAL_LINHAS, chunksize=CHUNKSIZE):
    total_chunks = TOTAL_LINHAS // chunksize + (1 if TOTAL_LINHAS % chunksize else 0)
    results = []

    with pd.read_csv(DATA_PATH, sep=';', header=None, names=['station', 'measure'], chunksize=CHUNKSIZE) as reader:
        # Envolvendo o iterador com tqdm para visualizar o progresso
        with Pool(CONCURRENCY) as pool:
            for chunk in tqdm(reader, total=total_chunks, desc="Processando"):
                # Processa cada chunk em paralelo
                result = pool.apply_async(process_chunk, (chunk,))
                results.append(result)

            results = [result.get() for result in results]

    final_df = pd.concat(results, ignore_index=True)

    final_aggregated_df = final_df.groupby('station').agg({
        'min': 'min',
        'max': 'max',
        'mean': 'mean'
    }).reset_index().sort_values('station')

    return final_aggregated_df

if __name__ == "__main__":
    import time

    print("Iniciando o processamento do arquivo.")
    start_time = time.time()
    df = create_df_with_pandas(DATA_PATH, TOTAL_LINHAS, CHUNKSIZE)
    took = time.time() - start_time

    print(df.head())
    print(f"Processing took: {took:.2f} sec")