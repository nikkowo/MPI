from mpi4py import MPI
import os, re, time
from collections import Counter

def leer_palabras(ruta):
    with open(ruta, 'r', encoding='utf-8') as f:
        texto = f.read().lower()
        return re.findall(r'\b[a-záéíóúüñ]+\b', texto)

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

directorio = '/app/target'
archivos = sorted([os.path.join(directorio, f) for f in os.listdir(directorio) if f.endswith('.txt')])

inicio = time.time()

# Proceso maestro
if rank == 0:
    print(f"Total de procesos: {size}")
    total_palabras = []

    # Distribuir archivos a los procesos trabajadores
    for i, archivo in enumerate(archivos[1:], start=1):
        destino = i % (size - 1) + 1
        comm.send(archivo, dest=destino)

    # Recibir resultados de los trabajadores
    for _ in range(1, size):
        data = comm.recv(source=MPI.ANY_SOURCE)
        total_palabras.extend(data)

    # Leer el archivo base
    palabras_base = leer_palabras(archivos[0])

    # Contar frecuencias globales
    conteo_global = Counter(total_palabras)
    conteo_base = {p: conteo_global[p] for p in palabras_base}

    # Top 5 palabras más frecuentes del file_01.txt
    top_5 = Counter(conteo_base).most_common(5)

    fin = time.time()
    print("\nCinco palabras más frecuentes en file_01.txt:")
    for palabra, freq in top_5:
        print(f"{palabra}: {freq} veces")
    print(f"\nTiempo total de ejecución: {fin - inicio:.4f} segundos")

# Procesos trabajadores
else:
    try:
        archivo = comm.recv(source=0)
        palabras = leer_palabras(archivo)
        comm.send(palabras, dest=0)
    except:
        comm.send([], dest=0)
