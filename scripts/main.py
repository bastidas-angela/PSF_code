import sqlite3
import funciones as fn
import traceback
import os

def eliminar_tablas(conn):
    """
    GESTIÓN DE TABLAS: Elimina tablas existentes tras confirmación del usuario.
    - Protege tablas críticas: EnergyConsumption, TrafficData, tarifas.
    - Muestra estructura y registros de cada tabla antes de eliminar.
    - Uso típico: reinicio de datos para pruebas o reprocesamiento.
    """
    cursor = conn.cursor()
    # Obtener nombres de todas las tablas definidas en la base de datos
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [row[0] for row in cursor.fetchall()]

    # Mostrar cada tabla y sus columnas para que el usuario vea su estructura
    print("\n🔍 **Tablas y sus columnas en la base de datos:**")
    for table in tables:
        cursor.execute(f"PRAGMA table_info({table})")
        columns = [row[1] for row in cursor.fetchall()]
        print(f"📋 Tabla '{table}': \n{', '.join(columns)}")
        
    # Filtrar tablas que no deben ser eliminadas
    tablas_protegidas = {"EnergyConsumption", "TrafficData","tarifas"}
    tables = [table for table in tables if table not in tablas_protegidas]

    # Preguntar si el usuario desea hacer cambios
    respuesta_general = input("\n❓ ¿Deseas realizar cambios en las tablas de la base de datos? (s/n): ").strip().lower()
    if respuesta_general == 's':
        # Para cada tabla, mostrar número de registros y preguntar si se desea eliminar
        print("\n🔍 **Tablas en la base de datos:**")
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"\n📊 La tabla '{table}' tiene {count} registros.")
            respuesta = input(f"❓ ¿Deseas eliminar la tabla '{table}'? (s/n): ").strip().lower()
            if respuesta == 's':
                cursor.execute(f"DROP TABLE {table}")
                print(f"✅ Tabla '{table}' eliminada.")
    else:
        print("\n🔒 No se realizaron cambios en las tablas.")

    # Guardar cambios en la base de datos
    conn.commit()


def main():
    """
    FLUJO PRINCIPAL DEL SCRIPT:
    1. Conexión a base de datos SQLite en carpeta específica de OneDrive.
    2. Ejecución secuencial de pasos de procesamiento (energía, tráfico, sitios, etc.).
    3. Exportación final a CSV, incluso si hay errores.
    """
    # Configuración de rutas (ajustar según entorno)
    base_name = "telecom_energy_universal.db"
    user_profile = os.environ.get("USERPROFILE") 
    folder_route = os.path.join(user_profile, "OneDrive", "Telefonica PSF", "Data")
    
    # Validación de ruta (evita errores críticos)
    if not os.path.isdir(folder_route):
        raise ValueError(f"Ruta inválida: {folder_route}")

    # Construir ruta completa al archivo SQLite
    base_route = os.path.join(folder_route, base_name)

    try:
        # Abrir conexión a la base de datos (se crea si no existe)
        conn = sqlite3.connect(base_route)
        print("\n✅ Conexión a la base de datos establecida correctamente.")

        # Paso opcional: eliminar tablas existentes para partir de cero
        #eliminar_tablas(conn)

        # Paso 2: procesar archivos de energía
        print("\n🔄 Procesando archivos de energía...")
        #fn.process_files(conn, os.path.join(folder_route, "DATA_NETECO"), "energy")
        print("✅ Procesamiento de archivos de energía completado.")

        # Paso 3: procesar archivos de tráfico
        print("\n🔄 Procesando archivos de tráfico...")
        #fn.process_files(conn, os.path.join(folder_route, "DATA_TRAFICO"), "traffic")
        print("✅ Procesamiento de archivos de tráfico completado.")

        # Paso 4: consolidar datos de sitios
        print("\n🔄 Consolidando datos de sitios...")
        fn.consolidar_datos_sitios(conn, os.path.join(folder_route, "DATA_SITIOS"))
        print("✅ Consolidación de datos de sitios completada.")

        # Paso 5: calcular etapas de los sitios
        print("\n🔄 Calculando etapas de los sitios...")
        #fn.calcular_etapas_sitios(conn, os.path.join(folder_route, "DATA_SITIOS"))
        print("✅ Cálculo de etapas de los sitios completado.")

        # Paso 6: análisis de consumo y actualización de etapas
        print("\n📊 Iniciando análisis de consumo...")
        #fn.calcular_consumo(conn)
        #fn.actualizar_etapas(conn)
        print("✅ Análisis de consumo completado.")

        # Paso 7: análisis de clusters
        print("\n📈 Iniciando análisis de clusters...")
        #fn.analisis_cluster(conn)
        print("✅ Análisis de clusters completado.")

        # Paso 8: análisis de tarifas y ahorro
        print("\n💰 Iniciando análisis de tarifas...")
        fn.analisis_tarifas(conn, os.path.join(folder_route, "DATA_TARIFAS"))
        print("\n💡 Cálculo de ahorro en proceso")
        fn.analisis_tarifas_ahorro_real_estimación_proyección(conn)
        fn.ahorro_proyectado_v2(conn)
        print("✅ Análisis de tarifas completado.")

        # Paso 9: cálculo final de ahorro proyectado
        print("\n💡 Calculando ahorro proyectado...")
        fn.ahorro_proyectado_v2(conn)
        print("✅ Cálculo de ahorro proyectado completado.")

    except Exception:
        # En caso de error, mostrar traza completa para depuración
        print("\n❌ **Error durante la ejecución:**")
        traceback.print_exc()

    finally:
        # Exportar toda la base de datos a CSV antes de cerrar conexión
        fn.export_sqlite_to_csv(conn, os.path.join(folder_route, "DATA_SALIDA"))
        conn.close()
        print("\n🔒 Conexión a la base de datos cerrada.")

# Ejecutar main solo si este script se corre directamente
if __name__ == "__main__":
    main()
