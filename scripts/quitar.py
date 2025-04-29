import os

# Ruta de la carpeta donde se encuentran los archivos
carpeta = r"C:\Users\ASUS\OneDrive\Telefonica PSF\Data\DATA_TARIFAS"

# Iterar sobre los archivos en la carpeta
for nombre_archivo in os.listdir(carpeta):
    if "_procesado" in nombre_archivo:
        # Crear el nuevo nombre del archivo
        nuevo_nombre = nombre_archivo.replace("_procesado", "")
        # Renombrar el archivo
        os.rename(
            os.path.join(carpeta, nombre_archivo),
            os.path.join(carpeta, nuevo_nombre)
        )

print("Renombrado completado.")