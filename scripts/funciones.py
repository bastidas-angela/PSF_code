import os
import re
import pandas as pd
import numpy as np
from datetime import date
import warnings

warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")
pd.set_option('future.no_silent_downcasting', True)

user_profile = os.environ.get("USERPROFILE")
folder_route = os.path.join(user_profile, "OneDrive","Telefonica PSF","Data") ### modifica con la carpeta de Dalia

def process_files(conn, data_folder, file_type):

    table_name = "EnergyConsumption" if file_type == "energy" else "TrafficData"

    files = []
    for root, _, filenames in os.walk(data_folder):
        if file_type == "energy":
            files.extend([os.path.join(root, f) for f in filenames if f.endswith(".xlsx") and "_procesado" not in f])
        else:
            files.extend([os.path.join(root, f) for f in filenames if (f.endswith(".xlsx") or f.endswith(".csv")) and "_procesado" not in f])
    
    if not files:
        print(f"No hay archivos nuevos para procesar de tipo {file_type}.")
        total_records = pd.read_sql_query(f"SELECT COUNT(*) AS total FROM {table_name}", conn)
        site_count = pd.read_sql_query(f"SELECT COUNT(DISTINCT SiteID) AS site_count FROM {table_name}", conn)
        date_range = pd.read_sql_query(f"""
            SELECT MIN(Timestamp) AS start_date, 
                   MAX(Timestamp) AS end_date 
            FROM {table_name}
        """, conn)
        
        print(f"Registros en la tabla '{table_name}': {total_records['total'][0]}")
        print(f"Sitios únicos: {site_count['site_count'][0]}")
        print(f"Información desde {date_range['start_date'][0]} hasta {date_range['end_date'][0]}")
        return
    
    consolidated_data = []
    print(f"Los archivos son: {files}")
    i=1

    for file in files:
        print(f"Procesando archivo [{i}/{len(files)}]: {file}")
        
        try:
            if file_type == "energy":
                data = pd.read_excel(file, 
                                     sheet_name="1 hour", 
                                     skiprows=5, 
                                     engine="openpyxl")
            else:
                if file.endswith(".xlsx"):
                    data = pd.read_excel(file, 
                                         sheet_name="Sheet 1", 
                                         engine="openpyxl")
                elif file.endswith(".csv"):
                    data = pd.read_csv(file)

            if isinstance(data, dict):
                print("🔍 Warning: El archivo tiene múltiples hojas. Seleccionando la primera disponible.")
                sheet_names = list(data.keys())
                print(f"Hojas disponibles: {', '.join(sheet_names)}")
                data = list(data.values())[0]  # Selecciona la primera hoja como DataFrame
            print("Archivo leido")

        except ValueError:
            print(f"La hoja especificada no se encontró en {file}. Saltando...")
            continue
        
        if file_type == "energy":
            data.rename(columns={
                "Site Name": "SiteID",
                "Start Time": "Timestamp",
                "Energy Consumption per Hour/kWh": "Consumption"
            }, inplace=True)
            selected_columns = ["SiteID", "Timestamp", "Consumption"]
            data["SiteID"] = data["SiteID"].apply(lambda x: x.split('_')[0])

        elif file_type == "traffic":
            meses = {
                "enero": "01", "febrero": "02", "marzo": "03", "abril": "04", "mayo": "05", "junio": "06",
                "julio": "07", "agosto": "08", "septiembre": "09", "octubre": "10", "noviembre": "11", "diciembre": "12"
            }

            # Extraer el día, el mes en texto y el año
            data[['Día', 'MesTexto', 'Año']] = data["Mes, Día, Año de Fecha"].str.extract(r"(\d+) de (\w+) de (\d+)")
            data["Mes"] = data["MesTexto"].map(meses)
            data["Date"] = data["Día"] + "-" + data["Mes"] + "-" + data["Año"]
            data["Date"] = pd.to_datetime(data["Date"], format="%d-%m-%Y")

            data.rename(columns={
                "Unico": "SiteID",
                "Hora de Fecha": "Hour",
                "Trafico Datos": "TrafficData"
            }, inplace=True)

            data["Timestamp"] = pd.to_datetime(data["Date"].astype(str) + " " + data["Hour"].astype(str) + ":00:00")
            selected_columns = ["SiteID", "Timestamp", "TrafficData"]
        else:
            print("Tipo de archivo no reconocido. Usar 'energy' o 'traffic'.")
            return
        
        data = data[selected_columns]
        if file_type == "traffic":
            data["Timestamp"] = pd.to_datetime(data["Timestamp"], dayfirst=True)
            print(f"Fecha menor: {data['Timestamp'].min()}")
            print(f"Fecha mayor: {data['Timestamp'].max()}")

        else:
            data["Timestamp"] = pd.to_datetime(data["Timestamp"])
        consolidated_data.append(data)
        i+=1
    
    if not consolidated_data:
        print(f"No se encontraron datos válidos para procesar de tipo {file_type}.")
        return
    
    consolidated_data = pd.concat(consolidated_data, ignore_index=True).drop_duplicates(subset=["SiteID", "Timestamp"])
    
    table_exists_query = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';"
    table_exists = pd.read_sql_query(table_exists_query, conn)

    if table_exists.empty:
        print(f"La tabla '{table_name}' no existe. Creándola...")
        consolidated_data.to_sql(table_name, conn, if_exists="replace", index=False)
        print(f"Tabla '{table_name}' creada exitosamente.")

    existing_data_query = f"SELECT SiteID, Timestamp FROM {table_name}"
    existing_data = pd.read_sql_query(existing_data_query, conn)
    
    existing_data["Timestamp"] = pd.to_datetime(existing_data["Timestamp"])
    merged_data = consolidated_data.merge(existing_data, on=["SiteID", "Timestamp"], how="inner")
    
    if not merged_data.empty:
        print(f"{len(merged_data)} registros duplicados encontrados en {table_name}. Serán ignorados.")
        consolidated_data = consolidated_data[~consolidated_data.set_index(["SiteID", "Timestamp"]).index.isin(merged_data.set_index(["SiteID", "Timestamp"]).index)]
    
    if not consolidated_data.empty:
        print(f"Cargando datos en la tabla '{table_name}'...")
        consolidated_data.to_sql(table_name, conn, if_exists="append", index=False)
        print(f"{len(consolidated_data)} registros nuevos cargados en la base de datos.")
    else:
        print("No hay datos nuevos para cargar.")
    
    table_exists_query = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';"
    table_exists = pd.read_sql_query(table_exists_query, conn)
    
    if table_exists.empty:
        print(f"La tabla '{table_name}' no existe o está vacía.")
    else:
        total_records = pd.read_sql_query(f"SELECT COUNT(*) AS total FROM {table_name}", conn)
        site_count = pd.read_sql_query(f"SELECT COUNT(DISTINCT SiteID) AS site_count FROM {table_name}", conn)
        date_range = pd.read_sql_query(f"""
            SELECT MIN(Timestamp) AS start_date, 
                   MAX(Timestamp) AS end_date 
            FROM {table_name}
        """, conn)
        
        print(f"Registros en la tabla '{table_name}': {total_records['total'][0]}")
        print(f"Sitios únicos: {site_count['site_count'][0]}")
        print(f"Información desde {date_range['start_date'][0]} hasta {date_range['end_date'][0]}")

    os.makedirs(os.path.join(data_folder, "old"), exist_ok=True)
    today = pd.Timestamp.today().strftime("%Y%m%d")
    for file in files:
        file_name = os.path.basename(file)
        os.rename(file, os.path.join(data_folder, "old", file_name.replace(".xlsx", f"_procesado_{today}.xlsx")))

def consolidar_datos_sitios(conn, data_folder):

    site_ids = pd.read_sql_query("SELECT DISTINCT SiteID FROM EnergyConsumption", conn)
    try:
        site_ids_tarifas = pd.read_sql_query("SELECT DISTINCT SiteID FROM Tarifas", conn)
    except pd.io.sql.DatabaseError:
        site_ids_tarifas = pd.DataFrame(columns=["SiteID"])
    files = os.listdir(data_folder)
    
    file_patterns = {
        "base_sitios": "base de sitios",
        "clusters": "cluster",
        "swap_fechas": "swap",
        "regiones": "regiones",
        "clasificaciones": "configuraciones_transformada_2",
        "clasificaciones_correo": "configuraciones_correo",
        "cuadro_fuerza": "info_cuadro_fuerza",
        "planning": "info_sitios_planning"
    }
    
    file_paths = {key: next((os.path.join(data_folder, f) for f in files if pattern in f.lower()), None) for key, pattern in file_patterns.items()}
    
    required_files = ["base_sitios", "clusters", "swap_fechas"]
    if any(file_paths[key] is None for key in required_files):
        raise FileNotFoundError("No se encontraron todos los archivos requeridos en la carpeta.")
    
    def cargar_archivo(path, sheet_name=None, columns=None):
        df = pd.read_excel(path, sheet_name=sheet_name) if sheet_name else pd.read_excel(path)
        df.columns = df.columns.str.replace("\n", " ", regex=True)  # Reemplaza saltos de línea con espacio
        df.columns = df.columns.str.strip().str.replace(r"\s+", " ", regex=True)  # Elimina espacios extra
        df.columns = df.columns.str.replace(r"[^\w\s]", "", regex=True)  # Elimina caracteres especiales
        df = df.reset_index()

        return df.rename(columns=columns)[list(columns.values())].drop_duplicates(subset="SiteID", keep="last") if columns else df
    
    base_sitios = cargar_archivo(file_paths["base_sitios"], "Base de Sitios", {
        "Codigo Unico": "SiteID",
        "Nombre Local": "NombreLocal",
        "Departamento": "Departamento",
        "Proveedor FLM": "ProveedorFLM",
        "Tipo Estacion": "TipoEstacion"
    })
    
    clusters = cargar_archivo(file_paths["clusters"], columns={
        "CU": "SiteID",
        "Cluster": "Cluster"
    })
    
    swap_fechas = cargar_archivo(file_paths["swap_fechas"], columns={
        "Codigo Estacion": "SiteID",
        "Fecha Fin Swap": "FechaFinSwap"
    })
    
    regiones = cargar_archivo(file_paths["regiones"], columns={
        "UNIQUE CODE": "SiteID",
        "REGION": "Region_Asia"
    }) if file_paths["regiones"] else pd.DataFrame()
    
    clasificaciones_correo = cargar_archivo(file_paths["clasificaciones_correo"], columns={
        "SiteID": "SiteID",
        "TIPO CLASS": "Classification"
    }) if file_paths["clasificaciones_correo"] else pd.DataFrame()
    
    cuadro_fuerza = cargar_archivo(file_paths["cuadro_fuerza"], columns={
        "Nemónico Código único": "SiteID",
        "CF": "Cuadro_Fuerza"
    }) if file_paths["cuadro_fuerza"] else pd.DataFrame()
    
    planning = cargar_archivo(file_paths["planning"], columns={
        "CODIGO_UNICO": "SiteID",
        "ZONA": "Zona", 
        "PLANNING_DESP": "Planning_Desp", 
        "Mes_Despliegue": "Mes_Despliegue",
        "Etiqueta_Control_Proyecto": "TipoProyecto"
    }) if file_paths["planning"] else pd.DataFrame()
    
    consolidated_data = base_sitios.merge(clusters, on="SiteID", how="left")
    consolidated_data = consolidated_data.merge(swap_fechas, on="SiteID", how="left")
    consolidated_data = consolidated_data.merge(regiones, on="SiteID", how="left") if not regiones.empty else consolidated_data
    consolidated_data = consolidated_data.merge(clasificaciones_correo, on="SiteID", how="left") if not clasificaciones_correo.empty else consolidated_data
    consolidated_data = consolidated_data.merge(cuadro_fuerza, on="SiteID", how="left") if not cuadro_fuerza.empty else consolidated_data
    consolidated_data = consolidated_data.merge(planning, on="SiteID", how="left") if not planning.empty else consolidated_data
    
    consolidated_data["En_Neteco"] = consolidated_data["SiteID"].isin(site_ids["SiteID"])
    consolidated_data["En_Tarifas"] = consolidated_data["SiteID"].isin(site_ids_tarifas["SiteID"])

    # Cambiar mes despliegue por números de mes
    meses = {
        "ENERO": 1, "FEBRERO": 2, "MARZO": 3, "ABRIL": 4, "MAYO": 5, "JUNIO": 6,
        "JULIO": 7, "AGOSTO": 8, "SEPTIEMBRE": 9, "OCTUBRE": 10, "NOVIEMBRE": 11, "DICIEMBRE": 12
    }
    consolidated_data["Mes_Despliegue"] = consolidated_data["Mes_Despliegue"].map(meses)

    # Lógica para la columna "AGREGAR_A_NETECO"
    def clasificar_sitios(row):
        
        if not pd.isna(row["Planning_Desp"]):
            if not row["En_Neteco"]:
                if row["Cuadro_Fuerza"] == "Huawei":
                    if not pd.isna(row["FechaFinSwap"]):
                        return "3. IMPORTANTE (CON SWAP)"
                    elif row["Mes_Despliegue"] in [3,4]:
                        return "1. URGENTE"
                    elif row["Mes_Despliegue"] in [5, 6]:
                        return "2. IMPORTANTE"
                    else:
                        return "4. PLANIFICACIÓN"
                else:
                    return "5. No Huawei"
            else:
                return "6. Ya integrado"
        else:
            return ""

    consolidated_data["AGREGAR_A_NETECO"] = consolidated_data.apply(clasificar_sitios, axis=1)

    # Imprimir todas las columnas unidas con comas
    columnas_unidas = ", ".join(consolidated_data.columns)
    print(f"Columnas de la tabla de sitios: {columnas_unidas}")

    consolidated_data.to_sql("SiteInfo", conn, if_exists="replace", index=False)
    
    print(f"Tabla 'SiteInfo' creada exitosamente. Total registros: {len(consolidated_data)}")
    print(f"Sitios con FechaFinSwap no vacía: {consolidated_data['FechaFinSwap'].notna().sum()}")

def calcular_etapas_sitios(conn, data_folder):
    excel_path = os.path.join(data_folder, "SITE_ETAPAS.xlsx")
    
    # 1. Leer la información consolidada de la base de datos
    query_sitios = "SELECT SiteID, FechaFinSwap FROM SiteInfo"
    sitios_data = pd.read_sql_query(query_sitios, conn)
    
    query_energia = """
        SELECT SiteID, 
               MIN(`Timestamp`) AS FechaInicioEnergia, 
               MAX(`Timestamp`) AS FechaMaxEnergia 
        FROM EnergyConsumption 
        GROUP BY SiteID
    """
    energia_data = pd.read_sql_query(query_energia, conn)
    
    # 2. Fusionar información
    sitios_data = sitios_data.merge(energia_data, on="SiteID", how="left")
    sitios_data["FechaFinSwap"]       = pd.to_datetime(sitios_data["FechaFinSwap"]).dt.date
    sitios_data["FechaInicioEnergia"] = pd.to_datetime(sitios_data["FechaInicioEnergia"]).dt.date
    sitios_data["FechaMaxEnergia"]    = pd.to_datetime(sitios_data["FechaMaxEnergia"]).dt.date
    
    # 3. Fechas de referencia (según tu tabla “corregida”)
    fecha_corte_1 = date(2024, 11, 8)   #  8-Nov-24
    fecha_swap_end= date(2024, 11, 19)  # 19-Nov-24
    fecha_legacy_start = date(2024, 11, 20)  # 20-Nov-24
    fecha_legacy_end   = date(2025, 1, 27)   # 27-Ene-25
    fecha_legacy_ps    = date(2025, 1, 28)   # 28-Ene-25
    
    etapas = []

    for _, row in sitios_data.iterrows():
        site_id             = row["SiteID"]
        fecha_inicio_energia= row["FechaInicioEnergia"]
        fecha_fin_swap      = row["FechaFinSwap"]
        fecha_max_energia   = row["FechaMaxEnergia"]
        
        # Validaciones básicas
        if pd.isna(fecha_inicio_energia) or pd.isna(fecha_max_energia):
            # Si no tenemos fechas de energía, no generamos etapas
            continue
        
        # -----------------------------------------------------------
        # ETAPA: SIN SWAP (si existe fecha_fin_swap, va hasta el día anterior)
        # -----------------------------------------------------------
        if not pd.isna(fecha_fin_swap):
            fin_sin_swap = min(fecha_fin_swap - pd.Timedelta(days=1), fecha_max_energia)
        else:
            # Si no hay fecha_fin_swap, va hasta el fin de los datos
            fin_sin_swap = fecha_max_energia
        
        if fin_sin_swap >= fecha_inicio_energia:
            etapas.append({
                "SiteID"      : site_id,
                "Etapa"       : "Sin Swap",
                "FechaInicio" : fecha_inicio_energia,
                "FechaFin"    : fin_sin_swap
            })
        
        # Si no existe fecha_fin_swap, no hay más etapas que agregar
        if pd.isna(fecha_fin_swap):
            continue
        
        # -----------------------------------------------------------
        # CLASIFICACIÓN EN 3 CASOS
        # -----------------------------------------------------------
        
        # CASO 1: fecha_fin_swap < 8‑Nov‑24
        if fecha_fin_swap < fecha_corte_1:
            # Swap: fecha_fin_swap → 19‑Nov‑24
            inicio_swap = fecha_fin_swap
            fin_swap    = min(fecha_swap_end, fecha_max_energia)  # 19-Nov-24
            if fin_swap >= inicio_swap:
                etapas.append({
                    "SiteID"      : site_id,
                    "Etapa"       : "Swap 0 Features",
                    "FechaInicio" : inicio_swap,
                    "FechaFin"    : fin_swap
                })
            
            # Swap_Legacy: 20‑Nov‑24 → 27‑Ene‑25
            inicio_legacy = fecha_legacy_start  # 20-Nov-24
            fin_legacy    = min(fecha_legacy_end, fecha_max_energia)  # 27-Ene-25
            if fin_legacy >= inicio_legacy:
                etapas.append({
                    "SiteID"      : site_id,
                    "Etapa"       : "Swap 3 Legacy",
                    "FechaInicio" : inicio_legacy,
                    "FechaFin"    : fin_legacy
                })
            
            # Swap_Legacy_PS: 28‑Ene‑25 → fecha_max_energia
            if fecha_max_energia >= fecha_legacy_ps:
                etapas.append({
                    "SiteID"      : site_id,
                    "Etapa"       : "Swap 3 Legacy + PrSc",
                    "FechaInicio" : fecha_legacy_ps,
                    "FechaFin"    : fecha_max_energia
                })
        
        # CASO 2: 8‑Nov‑24 <= fecha_fin_swap < 28‑Ene‑25
        elif fecha_fin_swap < fecha_legacy_ps:
            # Swap_Legacy: fecha_fin_swap → 27‑Ene‑25
            inicio_legacy = fecha_fin_swap
            fin_legacy    = min(fecha_legacy_end, fecha_max_energia)
            if fin_legacy >= inicio_legacy:
                etapas.append({
                    "SiteID"      : site_id,
                    "Etapa"       : "Swap 3 Legacy",
                    "FechaInicio" : inicio_legacy,
                    "FechaFin"    : fin_legacy
                })
            
            # Swap_Legacy_PS: 28‑Ene‑25 → fecha_max_energia
            if fecha_max_energia >= fecha_legacy_ps:
                etapas.append({
                    "SiteID"      : site_id,
                    "Etapa"       : "Swap 3 Legacy + PrSc",
                    "FechaInicio" : fecha_legacy_ps,
                    "FechaFin"    : fecha_max_energia
                })
        
        # CASO 3: fecha_fin_swap >= 28‑Ene‑25
        else:
            # Aquí ya NO hay "Swap" ni "Swap_Legacy".
            # Solo “Swap_Legacy_PS” a partir de 28-Ene-25
            if fecha_max_energia >= fecha_legacy_ps:
                etapas.append({
                    "SiteID"      : site_id,
                    "Etapa"       : "Swap 3 Legacy + PrSc",
                    "FechaInicio" : fecha_fin_swap,
                    "FechaFin"    : fecha_max_energia
                })

    # 4. Convertir a DataFrame, ordenar y guardar
    etapas_df = pd.DataFrame(etapas)
    etapas_df = etapas_df.sort_values(by=["SiteID", "FechaInicio"]).reset_index(drop=True)
    
    # Guardar en la base de datos
    etapas_df.to_sql("SiteStages", conn, if_exists="replace", index=False)
    
    # Guardar en Excel
    etapas_df.to_excel(excel_path, index=False)
    
    print(f"Tabla 'SiteStages' recalculada y guardada. Total registros: {len(etapas_df)}")

def calcular_consumo(conn):
    # Leer datos una sola vez
    print("Leyendo datos desde la base de datos...")
    energy_data = pd.read_sql_query("SELECT * FROM EnergyConsumption", conn)
    site_info = pd.read_sql_query("SELECT * FROM SiteInfo", conn)
    traffic_info = pd.read_sql_query("SELECT * FROM TrafficData", conn)
    site_stages = pd.read_sql_query("SELECT * FROM SiteStages", conn)

    # Convertir fechas a datetime una sola vez
    energy_data["Timestamp"] = pd.to_datetime(energy_data["Timestamp"])
    site_info["FechaFinSwap"] = pd.to_datetime(site_info["FechaFinSwap"])
    traffic_info["Timestamp"] = pd.to_datetime(traffic_info["Timestamp"])
    site_stages["FechaInicio"] = pd.to_datetime(site_stages["FechaInicio"])
    site_stages["FechaFin"] = pd.to_datetime(site_stages["FechaFin"])

    results = []
    weeks = []

    for tipo_de_analisis, horas in [("24h", (0, 23)), ("Nocturno", (0, 6))]:
        print(f"Procesando datos para análisis: {tipo_de_analisis}...")

        # Filtrar datos según el tipo de análisis
        energy_filtered = energy_data[energy_data["Timestamp"].dt.hour.between(*horas)]
        traffic_filtered = traffic_info[traffic_info["Timestamp"].dt.hour.between(*horas)]
        multiplier = 24 if tipo_de_analisis == "24h" else 6

        # Agregar columna de fecha
        energy_filtered = energy_filtered.copy()
        energy_filtered["Date"] = energy_filtered["Timestamp"].dt.date

        traffic_filtered = traffic_filtered.copy()
        traffic_filtered["Date"] = traffic_filtered["Timestamp"].dt.date

        # Agrupar por día y sitio, escalando valores según tipo de análisis
        daily_energy = energy_filtered.groupby(["SiteID", "Date"])["Consumption"].mean().reset_index()
        daily_energy["Consumption"] *= multiplier

        daily_traffic = traffic_filtered.groupby(["SiteID", "Date"])["TrafficData"].mean().reset_index()
        daily_traffic["TrafficData"] *= multiplier

        # Unir consumo de energía con tráfico de datos
        daily_consumption = pd.merge(daily_energy, daily_traffic, on=["SiteID", "Date"], how="outer")
        daily_consumption["FinalDate"] = pd.to_datetime(daily_consumption["Date"])

        # Unir con información de etapas de cada sitio
        daily_consumption = daily_consumption.merge(site_stages, on="SiteID", how="left")
        daily_consumption = daily_consumption[
            (daily_consumption["FinalDate"] >= daily_consumption["FechaInicio"]) & 
            (daily_consumption["FinalDate"] <= daily_consumption["FechaFin"])
        ]

        # Calcular límites superior e inferior de consumo y tráfico
        for col in ["Consumption", "TrafficData"]:
            daily_consumption[f"Mean{col}"] = daily_consumption.groupby(["SiteID", "Etapa"])[col].transform("mean")
            daily_consumption[f"UpperLimit{col}"] = daily_consumption[f"Mean{col}"] * 1.2
            daily_consumption[f"LowerLimit{col}"] = daily_consumption[f"Mean{col}"] * 0.8
            daily_consumption[f"Original{col}"] = daily_consumption[col]

        # Ajustar valores fuera de los límites usando `clip`
        daily_consumption["Consumption"] = daily_consumption["Consumption"].clip(
            lower=daily_consumption["LowerLimitConsumption"],
            upper=daily_consumption["UpperLimitConsumption"]
        )
        daily_consumption["TrafficData"] = daily_consumption["TrafficData"].clip(
            lower=daily_consumption["LowerLimitTrafficData"],
            upper=daily_consumption["UpperLimitTrafficData"]
        )

        # Interpolación de datos
        def interpolate_site(group):
            for col in ["Consumption", "TrafficData"]:
                method = "akima" if group[col].notna().sum() >= 4 else "linear"
                group[col] = group[col].interpolate(method=method)
            return group

        daily_consumption = daily_consumption.groupby("SiteID").apply(interpolate_site)

        # Calcular eficiencia energética kW por GB
        daily_consumption["kWperGB_Original"] = np.where(
            (daily_consumption["OriginalConsumption"].notna()) & (daily_consumption["OriginalTrafficData"].notna()) & (daily_consumption["OriginalTrafficData"] != 0),
            daily_consumption["OriginalConsumption"] / daily_consumption["OriginalTrafficData"], np.nan
        )

        daily_consumption["kWperGB"] = np.where(
            (daily_consumption["Consumption"].notna()) & (daily_consumption["TrafficData"].notna()) & (daily_consumption["TrafficData"] != 0),
            daily_consumption["Consumption"] / daily_consumption["TrafficData"], np.nan
        )

        # Identificar la mejor semana solo si hay datos en todos los días de la semana
        daily_consumption["DayOfWeek"] = daily_consumption["FinalDate"].dt.weekday + 1  # Lunes=1, Domingo=7
        daily_consumption["WeekNumber"] = daily_consumption["FinalDate"].dt.isocalendar().week

        # Asegurar que 'SiteID' sea solo una columna, no índice
        daily_consumption = daily_consumption.reset_index(drop=True)

        # Aplicar el agrupamiento correctamente
        semana_ideal = daily_consumption.groupby(["SiteID", "Etapa"]).filter(lambda x: x["DayOfWeek"].nunique() == 7)
        semana_ideal = semana_ideal.groupby(["SiteID", "Etapa", "DayOfWeek"]).apply(
            lambda x: x.iloc[2:-2] if len(x) >= 6 else x.iloc[1:-1] if len(x) >= 3 else x
        ).groupby(level=["SiteID", "Etapa", "DayOfWeek"]).agg({  # Modificación aquí: usar level en groupby
            "Consumption": "mean",
            "TrafficData": "mean",
            "kWperGB": "mean"
        }).reset_index()

        daily_consumption["Analisis"] = tipo_de_analisis
        semana_ideal["Analisis"] = tipo_de_analisis
        semana_ideal["WeekNumber"] = "Prom"

        results.append(daily_consumption)
        weeks.append(semana_ideal)

    # Concatenar resultados y guardar en la base de datos
    final_data = pd.concat(results, ignore_index=True)
    final_data["Grafico"] = "Data"
    final_weeks = pd.concat(weeks, ignore_index=True)
    final_weeks["Grafico"] = "Promedios"

    final_data = pd.concat([final_data, final_weeks], ignore_index=True)

    final_data.to_sql("data", conn, if_exists="replace", index=False)
    final_weeks.to_sql("semana_ideal", conn, if_exists="replace", index=False)

    actualizar_etapas(conn)

    print("Datos de consumo guardados en 'data'.")
    print("Semana ideal guardada en 'semana_ideal'.")

def actualizar_etapas(conn):
    # Leer datos de la tabla semana_ideal
    semana_ideal = pd.read_sql_query("SELECT * FROM semana_ideal", conn)
    site_info = pd.read_sql_query("SELECT * FROM SiteInfo", conn)
    
    # Calcular promedio de consumo y tráfico por sitio y etapa
    promedio_etapas = semana_ideal.groupby(["Analisis", "SiteID", "Etapa"]).agg({
        "Consumption": "mean",
        "TrafficData": "mean",
        "kWperGB": "mean"
    }).reset_index()
    
    # Detectar sitios fuera de lo normal (OUTLIER)
    outlier_sites = []
    for site in promedio_etapas["SiteID"].unique():
        site_data = promedio_etapas[promedio_etapas["SiteID"] == site]
        etapas_swap = site_data[site_data["Etapa"] != "SinSwap"]
        
        if not etapas_swap.empty:
            consumo_antes = site_data[site_data["Etapa"] == "SinSwap"]["Consumption"].values
            consumo_despues = etapas_swap["Consumption"].values
            
            if consumo_antes.size > 0 and any(consumo_despues > consumo_antes[0]):
                outlier_sites.append(site)
    
    promedio_etapas["Outlier"] = promedio_etapas["SiteID"].apply(lambda x: "Si" if x in outlier_sites else "No")
    
    # Guardar la tabla con promedios en la base de datos
    promedio_etapas.to_sql("promedio_etapas", conn, if_exists="replace", index=False, chunksize=2000)
    
    # Identificar qué etapas están presentes en cada sitio
    etapas_disponibles = promedio_etapas[promedio_etapas["Analisis"] == "24h"].pivot(index="SiteID", columns="Etapa", values="Consumption").notna().astype(str)
    etapas_disponibles.replace({"True": "Si", "False": "No"}, inplace=True)
    
    # Actualizar SiteInfo con columnas indicando presencia de etapas y marcando outliers
    site_info = site_info.merge(etapas_disponibles, on="SiteID", how="left")
    site_info["Outlier"] = site_info["SiteID"].apply(lambda x: "Si" if x in outlier_sites else "No")
    
    # Guardar la tabla actualizada en la base de datos
    site_info.to_sql("SiteInfo", conn, if_exists="replace", index=False, chunksize=2000)
    
    print("Tabla 'promedio_etapas' creada y datos actualizados en 'SiteInfo'. Se han identificado outliers.")

def analisis_cluster(conn): 
    print("Leyendo datos de la base de datos...")

    # Leer SiteInfo y semana_ideal solo una vez
    site_info = pd.read_sql_query("SELECT SiteID, Cluster FROM SiteInfo", conn)
    semana_ideal = pd.read_sql_query("SELECT * FROM semana_ideal", conn)

    # Almacenar resultados de ambos análisis
    resultados_cluster = []
    resultados_ahorro_sitios = []
    resultados_ahorro_cluster = []

    for tipo_de_analisis in ["24h", "Nocturno"]:
        print(f"Procesando análisis de clusters para: {tipo_de_analisis}...")

        # Filtrar la tabla semana_ideal según el tipo de análisis
        semana_ideal_filtrada = semana_ideal[semana_ideal["Analisis"] == tipo_de_analisis].copy()

        # Agregar información de Cluster
        semana_ideal_filtrada = semana_ideal_filtrada.merge(site_info, on="SiteID", how="left")

        # **Semana ideal por Cluster y Etapa**
        semana_ideal_cluster = (
            semana_ideal_filtrada.groupby(["Cluster", "Etapa", "DayOfWeek"])
            .apply(lambda x: x.sort_values("Consumption").iloc[1:-1] if len(x) > 3 else x)
            .groupby(level=["Cluster", "Etapa", "DayOfWeek"])
            .agg({"Consumption": "mean", "TrafficData": "mean", "kWperGB": "mean"})
            .reset_index()
        )
        semana_ideal_cluster["Analisis"] = tipo_de_analisis
        resultados_cluster.append(semana_ideal_cluster)

        # **Promedio de cada sitio en cada etapa**
        promedio_sitios = (
            semana_ideal_filtrada.groupby(["SiteID", "Etapa"])
            .agg({"Consumption": "mean", "TrafficData": "mean", "kWperGB": "mean"})
            .reset_index()
        )
         ## Resultado esperado: ['SiteID', 'Etapa', 'Consumption', 'TrafficData', 'kWperGB']

        # **Cálculo del ahorro por etapa**
        ahorro_etapas = []
        i=1

        for tipo in ['Consumption', 'TrafficData', 'kWperGB']:

            ahorros = []
            sin_swap = promedio_sitios[(promedio_sitios['Etapa'] == 'Sin Swap') & (promedio_sitios[f"{tipo}"].notna())] [['SiteID', f"{tipo}"]]
            etapas = promedio_sitios[(promedio_sitios['Etapa'] != 'Sin Swap') & (promedio_sitios[f"{tipo}"].notna())] [['SiteID', 'Etapa', f"{tipo}"]]
            ahorros = pd.merge(etapas, sin_swap, on=['SiteID'], suffixes=('', '_base'), how='inner')
            # Resultado esperado: ['SiteID', 'Etapa', 'Consumption', 'Consumption_base']
            ahorros['Etapas_comparadas'] = ahorros['Etapa'] + ' vs Sin Swap'
            ahorros[f"Ahorro_{tipo}"]=(ahorros[f"{tipo}_base"] - ahorros[f"{tipo}"]) / ahorros[f"{tipo}_base"]
            # Resultado esperado ['SiteID', 'Etapa', 'Consumption', 'Consumption_base', 'Etapas comparadas', 'Ahorro_Consumption']
            
            swap_legacy_ps = promedio_sitios[(promedio_sitios['Etapa'] == 'Swap 3 Legacy + PrSc') & (promedio_sitios[f"{tipo}"].notna())] [['SiteID', f"{tipo}"]]
            swap_legacy = promedio_sitios[(promedio_sitios['Etapa'] == 'Swap 3 Legacy') & (promedio_sitios[f"{tipo}"].notna())] [['SiteID', f"{tipo}"]]
            ahorros_2 = pd.merge(swap_legacy_ps,swap_legacy, on=['SiteID'], suffixes=('', '_base'), how='inner')
            ahorros_2['Etapas_comparadas'] = 'Nuevo Feature vs Swap 3 Legacy'
            ahorros_2[f"Ahorro_{tipo}"]=(ahorros_2[f"{tipo}_base"] - ahorros_2[f"{tipo}"]) / ahorros_2[f"{tipo}_base"]

            ahorros = pd.concat([ahorros, ahorros_2], ignore_index=True)

            if i==1:
                ahorro_etapas = ahorros
            else:                
                ahorro_etapas=pd.merge(ahorro_etapas,ahorros, on=['SiteID','Etapas_comparadas'], how='outer')
            i = i+1

        ahorro_etapas_df = pd.DataFrame(ahorro_etapas)
        ahorro_etapas_df["Analisis"] = tipo_de_analisis
        resultados_ahorro_sitios.append(ahorro_etapas_df) 

        # **Cálculo del ahorro por Cluster**
        ahorro_cluster = ahorro_etapas_df.merge(site_info, on="SiteID", how="left")
        ahorro_cluster = (
            ahorro_cluster.groupby(["Cluster", "Etapa"])
            .agg({
            "Ahorro_kWperGB": "mean",
            "Ahorro_TrafficData": "mean",
            "Ahorro_Consumption": "mean"
            })
            .reset_index()
        )
        ahorro_cluster["Analisis"] = tipo_de_analisis
        resultados_ahorro_cluster.append(ahorro_cluster)

    # **Concatenar resultados de ambos análisis y guardar en la base**
    semana_ideal_cluster_final = pd.concat(resultados_cluster, ignore_index=True)
    ahorro_etapas_sitios_final = pd.concat(resultados_ahorro_sitios, ignore_index=True)
    ahorro_cluster_final = pd.concat(resultados_ahorro_cluster, ignore_index=True)

    semana_ideal_cluster_final.to_sql("semana_ideal_cluster", conn, if_exists="replace", index=False, chunksize=5000)
    ahorro_etapas_sitios_final.to_sql("ahorro_etapas_sitios", conn, if_exists="replace", index=False, chunksize=5000)
    ahorro_cluster_final.to_sql("ahorro_cluster", conn, if_exists="replace", index=False, chunksize=5000)

    print("Tablas 'semana_ideal_cluster', 'ahorro_etapas_sitios' y 'ahorro_cluster' creadas exitosamente.")

    return semana_ideal_cluster_final, ahorro_etapas_sitios_final, ahorro_cluster_final

def analisis_tarifas(conn, data_folder):
    print("Iniciando análisis de tarifas...")

    processed_folder = os.path.join(data_folder, "old")
    os.makedirs(processed_folder, exist_ok=True)
    valor_minimo = 100

    # Verificar si las tablas 'tarifas' y 'suministros_id' existen
    existing_tables = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table'", conn)
    existing_table_names = existing_tables["name"].tolist()

    if "tarifas" not in existing_table_names:
        print("La tabla 'tarifas' no existe en la base de datos.")
    else:
        tarifas_count = pd.read_sql_query("SELECT COUNT(*) AS count FROM tarifas", conn)["count"][0]
        site_id_count = pd.read_sql_query("SELECT COUNT(DISTINCT SiteID) AS count FROM tarifas", conn)["count"][0]
        print(f"La tabla 'tarifas' existe y tiene {tarifas_count} registros y {site_id_count} SiteID únicos.")

    if "suministros_id" not in existing_table_names:
        print("La tabla 'suministros_id' no existe en la base de datos.")
    else:
        suministros_id_count = pd.read_sql_query("SELECT COUNT(*) AS count FROM suministros_id", conn)["count"][0]
        suministros_site_id_count = pd.read_sql_query("SELECT COUNT(DISTINCT SiteID) AS count FROM suministros_id", conn)["count"][0]
        print(f"La tabla 'suministros_id' existe y tiene {suministros_id_count} registros y {suministros_site_id_count} SiteID únicos.")

    try:
        tarifas_actuales = pd.read_sql_query("SELECT * FROM tarifas", conn)
        print("Tabla tarifas leída correctamente.")
        columns_to_drop = [col for col in tarifas_actuales.columns if col in ["Periodo_x", "Tarifa_ajustada_x", "PeriodosDisponibles_x", "Periodo_y", "Tarifa_ajustada_y", "PeriodosDisponibles_y"]]
        tarifas_actuales.drop(columns=columns_to_drop, inplace=True)
    except:
        tarifas_actuales = pd.DataFrame()
    
    # Leer tabla de IDs de suministros si ya existe
    try:
        suministros_id = pd.read_sql_query("SELECT SiteID,SUMINISTRO_ACTUAL,ID_Suministro,Tipo_Tarifa,Servicios,Outlier FROM suministros_id", conn)
        print("Tabla suministros_id leída correctamente.")
    except:
        suministros_id = pd.DataFrame(columns=["SiteID", "SUMINISTRO_ACTUAL", "ID_Suministro","Tipo_Tarifa","Servicios","Outlier"])

    # **Identificar archivos relevantes**
    all_files = [f for f in os.listdir(data_folder) if f.endswith(".xlsx") and "_procesado" not in f]
    evolutivo_file = next((f for f in all_files if re.search(r"evolutivo", f, re.IGNORECASE)), None)
    report_files = [f for f in all_files if re.search(r"reporte", f, re.IGNORECASE)]
    manual_file = next((f for f in all_files if re.search(r"manual", f, re.IGNORECASE)), None)

    tarifas_combined = []

    # **Procesar archivo Evolutivo para actualizar la tabla de suministros**
    if evolutivo_file:
        file_path = os.path.join(data_folder, evolutivo_file)
        evolutivo_data = pd.read_excel(file_path)
        print(f"Archivo Evolutivo leído: {file_path}")
        evolutivo_data = evolutivo_data.rename(columns={"cod_unico_ing": "SiteID", "TARIFA": "Tipo_Tarifa", "SERVICIOS":"Servicios", "SUMINISTRO ACTUAL": "SUMINISTRO_ACTUAL"}).dropna(subset=["SiteID"])

        tarifas_data = evolutivo_data[evolutivo_data["indicador"] == "valor_venta"]
        tarifas_data = tarifas_data.melt(
            id_vars=["SiteID", "SUMINISTRO_ACTUAL", "DISTRIBUIDOR", "PROVEEDOR", "Tipo_Tarifa"],
            value_vars=[col for col in tarifas_data.columns if col.startswith("2024") or col.startswith("2025")],
            var_name="AñoMes", value_name="Tarifa"
        ).dropna(subset=["Tarifa"])
        tarifas_data["SUMINISTRO_ACTUAL"] = tarifas_data["SUMINISTRO_ACTUAL"].str.strip()
        tarifas_data["ID_Suministro"] = tarifas_data["SiteID"]+"_"+tarifas_data["SUMINISTRO_ACTUAL"]
        tarifas_data["Fuente"] = "Evolutivo"
        tarifas_data = tarifas_data[tarifas_data["Tarifa"] >= valor_minimo]
        
        tarifas_data = tarifas_data.reset_index(drop=True)
        tarifas_combined.append(tarifas_data)
        print(f"Cantidad de sitios: {len(tarifas_data['SiteID'].unique())}")

        # **Limpieza de datos: Eliminar espacios en blanco de los suministros**
        evolutivo_data["SUMINISTRO_ACTUAL"] = evolutivo_data["SUMINISTRO_ACTUAL"].str.strip()
        evolutivo_data["ID_Suministro"]=evolutivo_data["SiteID"]+"_"+evolutivo_data["SUMINISTRO_ACTUAL"]

        # **Actualizar la tabla de suministros_id**
        suministros_id = pd.concat([suministros_id, evolutivo_data[["ID_Suministro","SiteID", "SUMINISTRO_ACTUAL","Tipo_Tarifa", "Servicios"]]], ignore_index=True)
        suministros_id = suministros_id.drop_duplicates(subset=["ID_Suministro"])
        suministros_id.to_sql("suministros_id", conn, if_exists="replace", index=False, chunksize=5000)

    # **Procesar archivos de Reporte de Consumo**
    i=1
    for report_file in report_files:
        file_path = os.path.join(data_folder, report_file)
        report_data = pd.read_excel(file_path)
        print(f"Archivo leído: {file_path}")

        report_data = report_data.rename(columns={
            "cod_suministro": "SUMINISTRO_ACTUAL",
            "per_consumo": "AñoMes",
            "distribuidor": "DISTRIBUIDOR",
            "base_imponible": "Tarifa",
            "SUMINISTRO ACTUAL": "SUMINISTRO_ACTUAL",
            "PERÍODO CONSUMO": "AñoMes",
            "DISTRIBUIDOR": "DISTRIBUIDOR",
            "BASE IMPONIBLE": "Tarifa"
        })[["SUMINISTRO_ACTUAL", "AñoMes", "DISTRIBUIDOR", "Tarifa"]]
        report_data = report_data[report_data["SUMINISTRO_ACTUAL"].notna()]

        meses = {"Enero": "01", "Febrero": "02", "Marzo": "03", "Abril": "04",
            "Mayo": "05", "Junio": "06", "Julio": "07", "Agosto": "08",
            "Septiembre": "09", "Octubre": "10", "Noviembre": "11", "Diciembre": "12"}

        # Convertir la columna a formato YYYYMM
        report_data["AñoMes"] = report_data["AñoMes"].astype(str).apply(lambda x: x.split(", ")[1] + meses[x.split(", ")[0]] if "," in x else x)

        report_data["PROVEEDOR"] = report_data["DISTRIBUIDOR"]
        report_data = report_data.dropna(subset=["Tarifa"])

        report_data["SUMINISTRO_ACTUAL"] = report_data["SUMINISTRO_ACTUAL"].astype(str).str.strip()
        report_data = report_data.merge(suministros_id, how="left", on="SUMINISTRO_ACTUAL", suffixes=('', '_suministro'))
        report_data = report_data.dropna(subset=["SiteID"])
        report_data = report_data[["SiteID", "SUMINISTRO_ACTUAL", "DISTRIBUIDOR", "PROVEEDOR", "Tipo_Tarifa", "AñoMes", "Tarifa", "ID_Suministro"]]

        report_data["Fuente"] = "Reporte" + str(i)
        report_data = report_data[report_data["Tarifa"] >= valor_minimo]
        report_data = report_data.reset_index(drop=True)
        tarifas_combined.append(report_data)

        print(f"Cantidad de sitios: {len(report_data['SiteID'].unique())}")
        i = i+1

    # **Procesar archivo Manual**
    if manual_file:
        file_path = os.path.join(data_folder, manual_file)
        manual_data = pd.read_excel(file_path)
        print(f"Archivo leído: {file_path}")

        manual_data = manual_data.rename(columns={"SITE ID": "SiteID", "SUMINISTRO ACTUAL": "SUMINISTRO_ACTUAL"}).dropna(subset=["SiteID"])

        manual_data = manual_data.melt(
            id_vars=["SiteID", "SUMINISTRO_ACTUAL"],
            value_vars=[col for col in tarifas_data.columns if col.startswith("2024") or col.startswith("2025")],
            var_name="AñoMes", value_name="Tarifa"
        ).dropna(subset=["Tarifa"])
        manual_data["DISTRIBUIDOR"] = "Luz del Sur"
        manual_data["PROVEEDOR"] = "Luz del Sur"

        manual_data["SUMINISTRO_ACTUAL"] = manual_data["SUMINISTRO_ACTUAL"].astype(str).str.strip()
        manual_data = manual_data.merge(suministros_id, how="left", on="SUMINISTRO_ACTUAL", suffixes=('', '_suministro'))
        manual_data = manual_data[["SiteID", "SUMINISTRO_ACTUAL", "DISTRIBUIDOR", "PROVEEDOR", "Tipo_Tarifa", "AñoMes", "Tarifa", "ID_Suministro"]]

        manual_data["Fuente"] = "Manual"
        manual_data = manual_data[manual_data["Tarifa"] >= valor_minimo]
        manual_data = manual_data.reset_index(drop=True)
        tarifas_combined.append(manual_data)

        print(f"Cantidad de sitios: {len(manual_data['SiteID'].unique())}")

    # **Concatenar y verificar**
    if tarifas_combined:
        tarifas_combined_final = pd.concat(tarifas_combined, ignore_index=True)
        frames = [
            df
            for df in [tarifas_actuales, tarifas_combined_final]
            if not df.empty and not df.isna().all(axis=0).all()
        ]
        tarifas_actualizadas = pd.concat(frames, join="inner", ignore_index=True)
        tarifas_actualizadas = tarifas_actualizadas.drop_duplicates(subset=["ID_Suministro", "AñoMes"], keep="last")
    else:
        tarifas_actualizadas = tarifas_actuales 
        print("\nNo hay nuevos datos")

    # **Eliminar duplicados, priorizando Manual > Reportes > Evolutivo**
    tarifas_actualizadas["AñoMes"] = tarifas_actualizadas["AñoMes"].astype(int)
    tarifas_actualizadas = tarifas_actualizadas.sort_values(by=["SiteID", "AñoMes", "Fuente"], ascending=[True, True, False])
    tarifas_actualizadas = tarifas_actualizadas.drop_duplicates(subset=["ID_Suministro", "AñoMes"], keep="first")
    
    print(f"La cantidad de sitios son: {len(tarifas_actualizadas['SiteID'].unique())}")
    print("Columnas en tarifas_actualizadas:", tarifas_actualizadas.columns.tolist())

    site_info = pd.read_sql_query("SELECT SiteID, FechaFinSwap FROM SiteInfo", conn)
    tarifas_combined_final = tarifas_actualizadas [["SiteID", "ID_Suministro", "AñoMes", "Tarifa" ]] 

    # **Asegurar que FechaFinSwap está en formato datetime viene de la tabla site info
    tarifas_combined_final = tarifas_combined_final.merge(site_info, how="left", on="SiteID")

    # **Determinar periodos ANTES_SWAP, EN SWAP, POST_SWAP**
    # Convertir AñoMes a datetime asegurando formato YYYYMM
    tarifas_combined_final["AñoMes_dt"] = pd.to_datetime(
        tarifas_combined_final["AñoMes"].astype(str), format="%Y%m"
    )

    # Asegurar que FechaFinSwap es datetime y manejar NaT
    tarifas_combined_final["FechaFinSwap"] = pd.to_datetime(
        tarifas_combined_final["FechaFinSwap"], errors="coerce"  # Convierte inválidos a NaT
    )

    # Convertir ambas fechas a formato AñoMes (YYYYMM) para hacer comparaciones precisas
    tarifas_combined_final["AñoMes_str"] = tarifas_combined_final["AñoMes_dt"].dt.strftime("%Y%m")
    tarifas_combined_final["FechaFinSwap_str"] = tarifas_combined_final["FechaFinSwap"].dt.strftime("%Y%m")

    # Asignar períodos considerando solo año y mes
    tarifas_combined_final["Periodo"] = np.where(
        tarifas_combined_final["FechaFinSwap"].isna(),
        "SIN_SWAP",  # Período especial para sitios sin fecha de swap
        np.where(
            tarifas_combined_final["AñoMes_str"] < tarifas_combined_final["FechaFinSwap_str"],
            "ANTES_SWAP",
            np.where(
                tarifas_combined_final["AñoMes_str"] == tarifas_combined_final["FechaFinSwap_str"],
                "EN_SWAP",
                "POST_SWAP"
            )
        )
    )

    # Eliminar columnas auxiliares si no las necesitas
    tarifas_combined_final.drop(columns=["AñoMes_str", "FechaFinSwap_str"], inplace=True)

    # Agrupar por ID_Suministro y unir los periodos disponibles en una nueva columna
    periodos_disponibles = tarifas_combined_final.groupby("ID_Suministro")["Periodo"].apply(
        lambda x: '-'.join(sorted([periodo.replace('_SWAP', '') for periodo in x.unique()]))
    ).reset_index()
    periodos_disponibles.rename(columns={"Periodo": "PeriodosDisponibles"}, inplace=True)

    # Se convierten los valores y, en caso de error, se pasan a NaN.
    tarifas_combined_final["Tarifa"] = pd.to_numeric(tarifas_combined_final["Tarifa"], errors="coerce")
    tarifas_combined_final = tarifas_combined_final.dropna(subset=["Tarifa"])

    def calcular_promedio(grupo):
        # Aquí 'grupo' es una Series de valores numéricos correspondientes a 'Tarifa'
        if len(grupo) > 3:
            # Se eliminan el valor mínimo y el máximo para evitar extremos.
            grupo = grupo.nsmallest(n=len(grupo)-1, keep="first").nlargest(n=len(grupo)-2, keep="first")
        return grupo.mean()

    # Aplicar la función a la columna 'Tarifa' usando groupby y seleccionando la serie explícitamente.
    promedio_tarifas = tarifas_combined_final.groupby(["ID_Suministro", "Periodo"])["Tarifa"].agg(calcular_promedio).reset_index()

    def ajustar_limites(grupo, promedio_df):
        # Extraer el promedio correspondiente al grupo actual utilizando .loc
        promedio = promedio_df.loc[
            (promedio_df["ID_Suministro"] == grupo.name[0]) & (promedio_df["Periodo"] == grupo.name[1]),
            "Tarifa"
        ].iloc[0]
        limite_superior = promedio * 1.3
        limite_inferior = promedio * 0.7
        # Asignar el valor recortado usando .loc para evitar advertencias de SettingWithCopy
        grupo.loc[:, "Tarifa"] = grupo["Tarifa"].clip(lower=limite_inferior, upper=limite_superior)
        return grupo

    tarifas_combined_final = tarifas_combined_final.groupby(["ID_Suministro", "Periodo"]).apply(
        lambda g: ajustar_limites(g, promedio_tarifas)
    ).reset_index(drop=True)

    # Cambiar el nombre de la columna "Tarifa" a "Tarifa_ajustada"
    tarifas_combined_final.rename(columns={"Tarifa": "Tarifa_ajustada"}, inplace=True)
    
    tarifas_original = tarifas_actualizadas[["SiteID", "SUMINISTRO_ACTUAL", "DISTRIBUIDOR", "PROVEEDOR", "Tipo_Tarifa", "AñoMes", "Tarifa", "ID_Suministro", "Fuente"]]
    tarifas_actualizadas_2 = pd.merge(tarifas_original, tarifas_combined_final[["ID_Suministro", "AñoMes", "Periodo", "Tarifa_ajustada"]], on=["ID_Suministro", "AñoMes"], how="outer")
    tarifas_actualizadas_2 = tarifas_actualizadas_2.merge(periodos_disponibles, on="ID_Suministro", how="left")
    print(f"Columnas en tarifas_actualizadas_2: {tarifas_actualizadas_2.columns.tolist()}")
    tarifas_actualizadas_2.to_sql("tarifas", conn, if_exists="replace", index=False, chunksize=5000)

    print("Datos de tarifas actualizados en 'tarifas'.")
    
    # Leer las columnas SiteID y TipoEstacion de SiteInfo
    site_info = pd.read_sql_query("SELECT SiteID, TipoEstacion FROM SiteInfo", conn)
    print(f"Tamaño de la tabla suministros_id: {len(suministros_id)} filas")
    suministros_id = pd.merge(suministros_id, site_info, on="SiteID", how="left")
    print(", ".join(suministros_id.columns))
    suministros_id["Servicios"] = suministros_id["Servicios"].str.replace(" ", "", regex=False)
    suministros_id["Ahorro_PSF"] = np.where(
        (suministros_id["Servicios"].isin(["CAV-EBC", "EBC", "EBC-CATV", "ERMO-EBC"])),
        "Si",
        "No"
    )
    suministros_id["Servicios"] = np.where(suministros_id["Ahorro_PSF"] == "No", "Otros", suministros_id["Servicios"])
    info_sumi=tarifas_actualizadas_2[["DISTRIBUIDOR", "PROVEEDOR", "ID_Suministro", "PeriodosDisponibles"]]
    info_sumi = info_sumi.drop_duplicates(subset=["ID_Suministro"])
    suministros_id = pd.merge(suministros_id, info_sumi, on="ID_Suministro", how="left")

    suministros_id.to_sql("suministros_id", conn, if_exists="replace", index=False, chunksize=5000)

    # **Mover archivos procesados**
    for file in all_files:
        old_path = os.path.join(data_folder, file)
        new_path = os.path.join(processed_folder, file.replace(".xlsx", "_procesado.xlsx"))
        os.rename(old_path, new_path)
    

def analisis_tarifas_ahorro_real_estimación_proyección(conn):

    tarifas = pd.read_sql_query("SELECT ID_Suministro,AñoMes,Periodo,Tarifa_ajustada FROM tarifas", conn)
    suministros_id = pd.read_sql_query("SELECT * FROM suministros_id", conn)
    info_sitios = pd.read_sql_query("SELECT SiteID, FechaFinSwap, TipoProyecto FROM SiteInfo", conn)

    tarifas = tarifas.merge(suministros_id[["ID_Suministro","Ahorro_PSF","PeriodosDisponibles"]], on="ID_Suministro", how="left")

    ahorro_mensual = tarifas[(tarifas["PeriodosDisponibles"].str.contains("ANTES", na=False)) & (tarifas["PeriodosDisponibles"].str.contains("POST", na=False)) & (tarifas["Ahorro_PSF"] == "Si")].copy()

    promedio_antes_swap = ahorro_mensual[ahorro_mensual["Periodo"] == "ANTES_SWAP"].groupby("ID_Suministro")["Tarifa_ajustada"].mean().reset_index()
    promedio_antes_swap.rename(columns={"Tarifa_ajustada": "PromedioAntesSwap"}, inplace=True)
    promedio_antes_swap = promedio_antes_swap[['ID_Suministro', 'PromedioAntesSwap']]

    post_swap = ahorro_mensual[ahorro_mensual["Periodo"] == "POST_SWAP"].copy()
    post_swap.rename(columns={"Tarifa_ajustada": "TarifaPostSwap"}, inplace=True)
    ahorro_mensual_real = pd.merge(post_swap, promedio_antes_swap, on="ID_Suministro", how="left")
    ahorro_mensual_real = ahorro_mensual_real[(ahorro_mensual_real["PromedioAntesSwap"].notna()) & (ahorro_mensual_real["TarifaPostSwap"].notna())]
    ahorro_mensual_real["AhorroMensual_Real"] = ahorro_mensual_real["PromedioAntesSwap"] - ahorro_mensual_real["TarifaPostSwap"]
    ahorro_mensual_real = ahorro_mensual_real [['ID_Suministro', 'AñoMes', 'PromedioAntesSwap', 'TarifaPostSwap', 'AhorroMensual_Real']]

    porcentaje_ahorro_real = ahorro_mensual_real.groupby("ID_Suministro")["TarifaPostSwap"].mean().reset_index()
    porcentaje_ahorro_real.rename(columns={"TarifaPostSwap": "PromedioPostSwap"}, inplace=True)
    porcentaje_ahorro_real = porcentaje_ahorro_real.merge(promedio_antes_swap, on="ID_Suministro", how="left")
    porcentaje_ahorro_real["PorcentajeAhorro_Real"] = ((porcentaje_ahorro_real["PromedioAntesSwap"] - porcentaje_ahorro_real["PromedioPostSwap"]) / porcentaje_ahorro_real["PromedioAntesSwap"])
    porcentaje_ahorro_real = porcentaje_ahorro_real[['ID_Suministro', 'PorcentajeAhorro_Real']]

    #Hallamos sitios Outliers
    outlier_suministros = ahorro_mensual_real[(ahorro_mensual_real["AhorroMensual_Real"] < -650) | (ahorro_mensual_real["AhorroMensual_Real"] > 650)]["ID_Suministro"].unique()
    suministros_con_info = ahorro_mensual_real["ID_Suministro"].unique()
    suministros_id["Outlier_Suministro"] = suministros_id["ID_Suministro"].apply(lambda x: "Si" if x in outlier_suministros else ("No" if x in suministros_con_info else "Sin información"))

    ### Columnas de ahorro_mensual_real: [ID_Suministro, AñoMes, Periodo, TarifaPostSwap, Ahorro_PSF, PeriodosDisponibles, PromedioAntesSwap, AhorroMensual_Real, Outlier_Suministro]
    ### ahorro_mensual_real y porcentaje_ahorro_real contiene outliers y no outliers

    ######## Comenzamos a estimar lo faltante.

    #fecha_corte = pd.to_datetime(str(ahorro_mensual_real["AñoMes"].max()), format="%Y%m")
    fecha_corte = pd.Timestamp("2025-02-01")
    print(f"Fecha de corte: {fecha_corte}")

    info_sitios = info_sitios.dropna(subset=["FechaFinSwap"])
    info_sitios["FechaFinSwap"] = pd.to_datetime(info_sitios["FechaFinSwap"], errors="coerce")
    info_sitios = info_sitios.merge(suministros_id[['ID_Suministro', 'SiteID', 'Tipo_Tarifa', 'Servicios', 'Outlier_Suministro']], on="SiteID", how="left")
    info_sitios["ID_Suministro"] = info_sitios["ID_Suministro"].fillna(info_sitios["SiteID"] + "_sin_suministro")
    info_sitios["Outlier_Suministro"] = info_sitios["Outlier_Suministro"].fillna("Sin información")
    info_sitios["Tipo_Tarifa"] = info_sitios["Tipo_Tarifa"].fillna("Sin información")
    info_sitios["Servicios"] = info_sitios["Servicios"].fillna("Sin información")
    info_sitios["Rango"] = np.where(
        info_sitios["FechaFinSwap"] < fecha_corte,
        "Dentro de rango",
        "Fuera de rango"
    )
    columnas_unidas = ", ".join(info_sitios.columns)
    print(f"Columnas de info_sitios: {columnas_unidas}")

    info_sitios_dentro_rango = info_sitios[info_sitios["Rango"] == "Dentro de rango"]
    info_sitios_dentro_rango = info_sitios_dentro_rango.merge(porcentaje_ahorro_real, on="ID_Suministro", how="left")

    df = pd.merge( info_sitios_dentro_rango, ahorro_mensual_real, on="ID_Suministro", how="left" )

    fecha_corte = pd.Timestamp(fecha_corte)
    nuevas_filas = []
    
    for supply_id in df["ID_Suministro"].unique():
        sub_df = df[df["ID_Suministro"] == supply_id]
        fecha_swap = pd.to_datetime(sub_df["FechaFinSwap"].iloc[0])
        
        if fecha_swap < fecha_corte:
            start_str = (fecha_swap + pd.DateOffset(months=1)).strftime("%Y%m")
            end_str = fecha_corte.strftime("%Y%m")
            
            start_date = pd.to_datetime(start_str, format="%Y%m")
            end_date = pd.to_datetime(end_str, format="%Y%m")
            
            expected = pd.date_range(start=start_date, end=end_date, freq="MS").strftime("%Y%m").astype(str).tolist()
            existing = df[df["ID_Suministro"] == supply_id]["AñoMes"].astype(str).tolist()
            
            missing = set(expected) - set(existing)
            if missing:
                for missing_mes in sorted(missing):
                    nuevas_filas.append({
                        "SiteID": sub_df["SiteID"].iloc[0],
                        "TipoProyecto": sub_df["TipoProyecto"].iloc[0],
                        "FechaFinSwap": sub_df["FechaFinSwap"].iloc[0],
                        "ID_Suministro": supply_id,
                        "Tipo_Tarifa": sub_df["Tipo_Tarifa"].iloc[0],
                        "Servicios": sub_df["Servicios"].iloc[0],
                        "Outlier_Suministro": sub_df["Outlier_Suministro"].iloc[0],
                        "PorcentajeAhorro_Real": sub_df["PorcentajeAhorro_Real"].iloc[0],
                        "Rango": sub_df["Rango"].iloc[0],
                        "AñoMes": missing_mes,
                        "PromedioAntesSwap": sub_df["PromedioAntesSwap"].iloc[0],
                        "TarifaPostSwap": None,  # Se deja vacío
                        "AhorroMensual_Real": None  # Se deja vacío
                    })

    nuevas_filas_df = pd.DataFrame(nuevas_filas)
    nuevas_filas_df = nuevas_filas_df.drop_duplicates(subset=["SiteID", "ID_Suministro", "AñoMes"], keep="first")
    if not nuevas_filas_df.empty:
        cols = df.columns.tolist()                
        nuevas_filas_df = nuevas_filas_df[cols] 
           
        all_na_cols = [
            c for c in nuevas_filas_df.columns
            if nuevas_filas_df[c].isna().all()
        ]
        if all_na_cols:
            nuevas_filas_df[all_na_cols] = nuevas_filas_df[all_na_cols].astype("float64")

        info_completa = pd.concat([df, nuevas_filas_df], ignore_index=True)
        print("Se han creado filas para los AñoMes faltantes.")
    else:
        info_completa = df.copy()
        print("No se encontraron suministros con AñoMes faltantes.")
        
    info_completa = info_completa[info_completa["AñoMes"].notna()]
    info_completa["AñoMes"] = info_completa["AñoMes"].astype(int)
    info_completa = info_completa.drop_duplicates(subset=["ID_Suministro", "AñoMes"], keep="first")
    info_completa = info_completa.sort_values(by=["FechaFinSwap", "SiteID", "ID_Suministro", "AñoMes"]).reset_index(drop=True)

    # Etiquetar el tipo de ahorro
    info_completa["TipoAhorro"] = np.where( (info_completa["Outlier_Suministro"] == "No") & (info_completa["PorcentajeAhorro_Real"].notna()) & (info_completa["TarifaPostSwap"].notna()),
        "Confirmado",
        "Estimado"
    )
    df = info_completa.copy()

   # Calcular guías globales con datos confirmados de EBC y BT5B
    filtro_guia = (df["Servicios"] == "EBC") & (df["Tipo_Tarifa"] == "BT5B") & (df["TipoAhorro"] == "Confirmado") & (df["Outlier_Suministro"] == "No") 
    guia_df = df[filtro_guia].drop_duplicates(subset=["ID_Suministro"], keep="first")
    guia_df["PorcentajeAhorro_Real"] = pd.to_numeric(guia_df["PorcentajeAhorro_Real"], errors='coerce')
    guia_df["PromedioAntesSwap"] = pd.to_numeric(guia_df["PromedioAntesSwap"], errors='coerce')
    print(f"Cantidad de filas en guia_df: {len(guia_df)}")
    porcentaje_ahorro_guia = guia_df["PorcentajeAhorro_Real"].nsmallest(len(guia_df) - 30).nlargest(len(guia_df) - 60).mean() 
    promedio_antes_swap_guia = guia_df["PromedioAntesSwap"].nsmallest(len(guia_df) - 30).nlargest(len(guia_df) - 60).mean()

    print(f"Porcentaje de ahorro guía: {porcentaje_ahorro_guia:.2%}")
    print(f"Promedio de tarifa antes del swap guía: {promedio_antes_swap_guia:.2f}")

    df["AhorroMensual"] = None
    df["PorcentajeAhorro"] = None
    df["TarifaPostSwap_Estimada"] = None
    df["PromedioAntesSwap_Estimada"] = None

    # Agrupar por Servicios y Tipo_Tarifa
    grouped = df.groupby(["TipoProyecto", "Servicios", "Tipo_Tarifa"], dropna=False)

    for (tipo, servicio, tarifa), group in grouped:
        confirmado = group[group["TipoAhorro"] == "Confirmado"]
        confirmado = confirmado.drop_duplicates(subset=["ID_Suministro"], keep="first")

        if not confirmado.empty:
            # Calcular promedio con exclusión de extremos (outlier removal)
            for col in ["PorcentajeAhorro_Real", "PromedioAntesSwap"]:
                clean = confirmado[col].dropna()
                if len(clean) > 20:
                    clean = clean.nsmallest(len(clean) - 4).nlargest(len(clean) - 8)
                elif len(clean) > 10:
                    clean = clean.nsmallest(len(clean) - 2).nlargest(len(clean) - 4)
                elif len(clean) > 5:
                    clean = clean.nsmallest(len(clean) - 1).nlargest(len(clean) - 2)
                promedio = clean.mean()
                if col == "PorcentajeAhorro_Real":
                    promedio_porcentaje_ahorro = promedio
                else:
                    promedio_tarifa_antes = promedio
        else:
            # Usar guía global si no hay confirmados
            promedio_porcentaje_ahorro = porcentaje_ahorro_guia
            promedio_tarifa_antes = promedio_antes_swap_guia

        # Asignar para confirmados (rellenar valores faltantes)
        mask_conf = (group["TipoAhorro"] == "Confirmado")
        group.loc[mask_conf, "PorcentajeAhorro"] = group.loc[mask_conf, "PorcentajeAhorro_Real"]
        group.loc[mask_conf, "AhorroMensual"] = group.loc[mask_conf, "AhorroMensual_Real"]
        group.loc[mask_conf, "PromedioAntesSwap_Estimada"] = group.loc[mask_conf, "PromedioAntesSwap"]
        group.loc[mask_conf, "TarifaPostSwap_Estimada"] = group.loc[mask_conf, "TarifaPostSwap"]

        # Asignar para estimados
        mask_est = (group["TipoAhorro"] == "Estimado")
        
        porcentaje_ajuste = group.loc[mask_est, "PorcentajeAhorro_Real"].fillna(promedio_porcentaje_ahorro).infer_objects(copy=False)
        group.loc[mask_est, "PorcentajeAhorro"] = np.where(
            group.loc[mask_est, "Outlier_Suministro"] == "Si",
            promedio_porcentaje_ahorro,
            porcentaje_ajuste
        )

        promedios_ajuste = group.loc[mask_est, "PromedioAntesSwap"].fillna(promedio_tarifa_antes).infer_objects(copy=False)
        group.loc[mask_est, "PromedioAntesSwap_Estimada"] = np.where(
            group.loc[mask_est, "Outlier_Suministro"] == "Si",
            promedio_tarifa_antes,
            promedios_ajuste
        )

        group.loc[mask_est, "TarifaPostSwap_Estimada"] = (
            group.loc[mask_est, "PromedioAntesSwap_Estimada"] * (1 - group.loc[mask_est, "PorcentajeAhorro"])
        )

        group.loc[mask_est, "AhorroMensual"] = (
            group.loc[mask_est, "PromedioAntesSwap_Estimada"] - group.loc[mask_est, "TarifaPostSwap_Estimada"]
        )

        # Si el servicio es "Otros", limpiar valores
        group.loc[group["Servicios"] == "Otros", ["PorcentajeAhorro", "AhorroMensual"]] = [None, None]

        # Actualizar el DataFrame original
        df.update(group)

    # Limpiar los datos fuera de rango
    df.loc[df["Rango"] == "Fuera de rango", ["PorcentajeAhorro", "AhorroMensual"]] = [None, None]

    # Agrupar por ID_Suministro y calcular los promedios
    df_grouped = df.groupby("ID_Suministro").agg({
        "PorcentajeAhorro": "mean",
        "PorcentajeAhorro_Real": "mean",
        "PromedioAntesSwap": "mean",
        "PromedioAntesSwap_Estimada": "mean",
        "TarifaPostSwap": "mean",
        "TarifaPostSwap_Estimada": "mean"
    }).reset_index()

    # Renombrar las columnas para mayor claridad
    df_grouped.rename(columns={
        "PorcentajeAhorro": "Promedio_PorcentajeAhorro",
        "PorcentajeAhorro_Real": "Promedio_PorcentajeAhorroReal",
        "PromedioAntesSwap": "Promedio_AntesSwapReal",
        "PromedioAntesSwap_Estimada": "Promedio_AntesSwapEstimado",
        "TarifaPostSwap": "Promedio_PostSwapReal",
        "TarifaPostSwap_Estimada": "Promedio_PostSwapEstimado"
    }, inplace=True)

    # Clasificar ahorro en función de los tipos presentes en cada SiteID usando apply para pasar el DataFrame completo del grupo
    def clasificar_ahorro(grupo):
        tipos = set(grupo["TipoAhorro"].dropna())
        if tipos == {"Confirmado"}:
            return "Confirmado totalmente"
        elif tipos == {"Estimado"}:
            return "Estimado totalmente"
        elif "Confirmado" in tipos and "Estimado" in tipos:
            return "Confirmado más Estimado"
        else:
            return "Sin información"

    df_etiquetas = df.groupby("SiteID").apply(clasificar_ahorro).reset_index(name="Confirmado/Estimado")

    # Evaluar si un sitio tiene al menos un suministro con Outlier_Suministro = "Si"
    sitios_outlier = info_sitios.groupby("SiteID").agg({
        "Outlier_Suministro": lambda x: "Si" if "Si" in x.values 
                                        else "No" if set(x.dropna()) == {"No"} 
                                        else "Sin información" if set(x.dropna()) == {"Sin información"} 
                                        else "No"
    }).reset_index()
    sitios_outlier.rename(columns={"Outlier_Suministro": "Sitios_Outlier"}, inplace=True)

    # Función para obtener el valor máximo de PeriodosDisponibles (filtrando valores no string)
    def get_max_periodos(x):
        valid = [v for v in x if isinstance(v, str)]
        if valid:
            return max(valid, key=len)
        else:
            return None

    PeriodosDisponibles_sitios = suministros_id.groupby("SiteID").agg({
        "PeriodosDisponibles": get_max_periodos
    }).reset_index()

    # Evaluar si los ID_Suministro de info_sitios están en tarifas
    info_sitios["Suministro_Info"] = info_sitios["ID_Suministro"].apply(
        lambda x: "Con información" if x in tarifas["ID_Suministro"].values else "Sin información"
    )
    inf_en_tarifa = info_sitios.groupby("SiteID")["Suministro_Info"].apply(lambda x: "Con información" if "Con información" in x.values else "Sin información").reset_index()
    inf_en_tarifa.rename(columns={"Suministro_Info": "En_tarifa"}, inplace=True)

    # Obtener la lista única de SiteID de info_sitios
    Sitios = pd.DataFrame(info_sitios["SiteID"].unique(), columns=["SiteID"])

    # Unir todas las tablas en una sola salida final
    df_salida = (
        Sitios
        .merge(df_etiquetas, on="SiteID", how="left")
        .merge(sitios_outlier, on="SiteID", how="left")
        .merge(PeriodosDisponibles_sitios, on="SiteID", how="left")
        .merge(inf_en_tarifa, on="SiteID", how="left")
    )

    # Rellenar valores faltantes
    df_salida["Confirmado/Estimado"] = df_salida["Confirmado/Estimado"].fillna("Fuera de rango")
    df_salida["Sitios_Outlier"] = df_salida["Sitios_Outlier"].fillna("Sin información")
    df_salida["PeriodosDisponibles"] = df_salida["PeriodosDisponibles"].fillna("Sin información")
    df_salida["En_tarifa"] = df_salida["En_tarifa"].fillna("Sin información")

    # Guardar los resultados en la base de datos

    df.to_sql("ahorro_estimado_v2", conn, if_exists="replace", index=False)
    df_salida.to_sql("sitios_clasificados", conn, if_exists="replace", index=False)
    suministros_id.to_sql("suministros_id", conn, if_exists="replace", index=False)
    df_grouped.to_sql("promedios_por_suministro", conn, if_exists="replace", index=False)

def ahorro_proyectado_v2(conn):

    # Cargar datos desde SQL
    info_sitios = pd.read_sql_query("SELECT SiteID, FechaFinSwap, Planning_Desp, Mes_Despliegue, TipoProyecto, TipoEstacion FROM SiteInfo", conn)
    info_suministros = pd.read_sql_query("SELECT SiteID, ID_Suministro, Tipo_Tarifa, Servicios, Ahorro_PSF, PeriodosDisponibles FROM suministros_id", conn)
    ahorro = pd.read_sql_query("SELECT ID_Suministro,Promedio_PorcentajeAhorro,Promedio_AntesSwapEstimado,Promedio_PostSwapEstimado FROM promedios_por_suministro", conn)
    tarifas = pd.read_sql_query("SELECT ID_Suministro,AñoMes,Periodo,Tarifa_ajustada,PeriodosDisponibles FROM tarifas", conn)

    # Unir info_sitios y info_suministros por ID_Suministro
    merged_info = pd.merge(info_sitios, info_suministros, on="SiteID", how="left")
    merged_info = merged_info.dropna(subset=['Planning_Desp'])
    merged_info["ID_Suministro"] = merged_info["ID_Suministro"].fillna(merged_info["SiteID"] + "_sin_suministro")
    merged_info["Tipo_Tarifa"] = merged_info["Tipo_Tarifa"].fillna("Sin información")
    merged_info["Servicios"] = merged_info["Servicios"].fillna("Sin información")

    # Valores de sitios sin swap
    tarifas_SIN = tarifas[tarifas["PeriodosDisponibles"] == "SIN"].copy()
    tarifas_SIN = tarifas_SIN.groupby("ID_Suministro")["Tarifa_ajustada"].mean().reset_index()
    tarifas_SIN.rename(columns={"Tarifa_ajustada": "Promedio_SinSwap"}, inplace=True)
    merged_info = pd.merge(merged_info, tarifas_SIN[['ID_Suministro','Promedio_SinSwap']], on="ID_Suministro", how="left")

    # Unimos los valores de ahorro
    ahorro.rename(columns={
        "Promedio_PorcentajeAhorro": "PorcentajeAhorro",
        "Promedio_AntesSwapEstimado": "Promedio_AntesSwap",
        "Promedio_PostSwapEstimado": "Promedio_PostSwap"
    }, inplace=True)
    merged_info = pd.merge(merged_info, ahorro, on="ID_Suministro", how="left")

    Referencias = merged_info[["ID_Suministro", "TipoProyecto", "Servicios", "Tipo_Tarifa","Promedio_SinSwap","Promedio_AntesSwap","Promedio_PostSwap","PorcentajeAhorro"]]
    Referencias = Referencias.drop_duplicates(subset=["ID_Suministro"], keep="first")

    # Agrupa por TipoProyecto, Tipo_Tarifa, Servicios
    Referencias["Referencia_Previa"] = None
    Referencias["Referencia_Porcentaje"] = None
    Referencias["Ahorro_Esperado"] = None
    Referencias["Calculo"] = None

    # Agrupar por Servicios y Tipo_Tarifa
    grouped = Referencias.groupby(["TipoProyecto", "Servicios", "Tipo_Tarifa"], dropna=False)

    for (proyecto, servicio, tarifa), group in grouped:
        confirmado = group[group["PorcentajeAhorro"].notna() | group["Promedio_SinSwap"].notna()]
        if not confirmado.empty:
            # Calcular promedio con exclusión de extremos (outlier removal)
            for col in ["PorcentajeAhorro", "Referencia_Previa"]:

                if col == "PorcentajeAhorro":
                    clean = confirmado[col].dropna()
                    clean = clean.infer_objects(copy=False)
                    if clean.empty:
                        clean = pd.Series(dtype=float)
                else:
                    clean_antes = confirmado["Promedio_AntesSwap"].dropna()
                    clean_sin = confirmado["Promedio_SinSwap"].dropna()
                    if not clean_antes.empty and not clean_sin.empty:
                        clean = clean_antes.combine_first(clean_sin)
                    elif not clean_antes.empty:
                        clean = clean_antes
                    elif not clean_sin.empty:
                        clean = clean_sin
                    else:
                        # los dos vacíos
                        clean = pd.Series(dtype="float64")

                if len(clean) > 20:
                    clean = clean.nsmallest(len(clean) - 4).nlargest(len(clean) - 8)
                elif len(clean) > 10:
                    clean = clean.nsmallest(len(clean) - 2).nlargest(len(clean) - 4)
                elif len(clean) > 5:
                    clean = clean.nsmallest(len(clean) - 1).nlargest(len(clean) - 2)
                promedio = clean.mean()

                if col == "PorcentajeAhorro":
                    lenporcentaje = len(clean)
                    promedio_porcentaje_ahorro = promedio
                else:
                    lentarifa=len(clean)
                    promedio_tarifa_ref = promedio
            
            if pd.notna(promedio_porcentaje_ahorro) and pd.notna(promedio_tarifa_ref):
                calculo = "Si"
            else:
                calculo = "No"
        else:
            promedio_porcentaje_ahorro = None
            promedio_tarifa_ref = None
            calculo = "No"
        
        group.loc[:, "Calculo"] = calculo

        mask_antes = (group["Promedio_AntesSwap"].notna())
        group.loc[mask_antes, "Referencia_Previa"] = group.loc[mask_antes, "Promedio_AntesSwap"]
        mask_sin = (group["Promedio_SinSwap"].notna())
        group.loc[mask_sin, "Referencia_Previa"] = group.loc[mask_sin, "Promedio_SinSwap"]
        mask_porc = (group["PorcentajeAhorro"].notna())
        group.loc[mask_porc, "Referencia_Porcentaje"] = group.loc[mask_porc, "PorcentajeAhorro"]

        mask_si = group["Calculo"] == "Si"
        if promedio_porcentaje_ahorro is not None:
            group.loc[mask_si, "Referencia_Porcentaje"] = group.loc[mask_si, "Referencia_Porcentaje"].fillna(promedio_porcentaje_ahorro)
            group.loc[mask_si, "Referencia_Previa"] = group.loc[mask_si, "Referencia_Previa"].fillna(promedio_tarifa_ref)
            group.loc[mask_si, "Ahorro_Esperado"] = group.loc[mask_si, "Referencia_Previa"] * group.loc[mask_si, "Referencia_Porcentaje"]*0.9
        
        Referencias.update(group)

    # Imprimir las columnas de Referencias y merged_info
    print(f"Columnas de Referencias: {', '.join(Referencias.columns)}")
    print(f"Columnas de merged_info: {', '.join(merged_info.columns)}")
    Referencias = Referencias.merge(merged_info[["SiteID", "FechaFinSwap", "Planning_Desp", "Mes_Despliegue", "TipoEstacion", "ID_Suministro", "Ahorro_PSF", "PeriodosDisponibles"]], on="ID_Suministro", how="left")

    sitios_clasificados = pd.read_sql_query("SELECT SiteID, [Confirmado/Estimado] FROM sitios_clasificados", conn)
    Referencias = Referencias.merge(sitios_clasificados, on="SiteID", how="left")

    Referencias["Proyección"] = Referencias.groupby("SiteID")["Confirmado/Estimado"].transform( lambda x: "REAL" if x.isin(["Confirmado más Estimado", "Confirmado totalmente"]).any() else None )
    Referencias["Proyección"] = Referencias.apply( lambda row: "Estimado" if pd.isna(row["Proyección"]) and row["Calculo"] == "Si" else row["Proyección"], axis=1 )
    Referencias["Proyección"] = Referencias["Proyección"].fillna("Sin información")

    Ahorro_calculo = Referencias[["ID_Suministro", "Referencia_Previa", "Referencia_Porcentaje", "Ahorro_Esperado", "Planning_Desp", "Mes_Despliegue"]].copy()
    print(f"Columnas de la tabla Ahorro_calculo: {', '.join(Ahorro_calculo.columns)}")
    print(f"Tamaño de la tabla Ahorro_calculo: {len(Ahorro_calculo)} filas")

    for i in range(1, 13):
        Ahorro_calculo.loc[:, f'{i:02d}_2025'] = None
    
    for idx, row in Ahorro_calculo.iterrows():
        planning = str(row["Planning_Desp"]).lower()
        mes_despliegue = row["Mes_Despliegue"]
        avg_antes = row.get("Referencia_Previa")
        savings_pct = row.get("Referencia_Porcentaje")
        ahorro = row.get("Ahorro_Esperado")
        
        if pd.notnull(avg_antes) and pd.notnull(savings_pct):            
            if '2024' in planning:
                for mes in range(1, 13):
                    Ahorro_calculo.at[idx, f'{mes:02d}_2025'] = ahorro
            elif '2025' in planning and pd.notnull(mes_despliegue):
                start_month = int(mes_despliegue) + 1
                for mes in range(start_month, 13):
                    Ahorro_calculo.at[idx, f'{mes:02d}_2025'] = ahorro
    

    # Crear una nueva tabla para almacenar las columnas de ahorro proyectado por mes
    ahorro_proyectado_mensual = Ahorro_calculo[["ID_Suministro"] + [f'{i:02d}_2025' for i in range(1, 13)]].copy()

    ahorro_proyectado_mensual = ahorro_proyectado_mensual.melt(
        id_vars=["ID_Suministro"],
        value_vars=[f'{i:02d}_2025' for i in range(1, 13)],
        var_name="Mes_Año",
        value_name="Valor_Proyectado"
    ).dropna(subset=["Valor_Proyectado"])

    #Guardamos en base de datos
    ahorro_proyectado_mensual.to_sql("AHORRO_PROYECTADO_MENSUAL_v2", conn, if_exists="replace", index=False)
    Referencias.to_sql("AHORRO_PROYECTADO_v2", conn, if_exists="replace", index=False)

def export_sqlite_to_csv(conn, output_folder):
    """
    Exporta todas las tablas de una base de datos SQLite a archivos CSV.

    Parámetros:
    sqlite_db_path (str): Ruta a la base de datos SQLite.
    output_folder (str): Carpeta donde se guardarán los archivos CSV.
    """
    # Crear carpeta de salida si no existe
    os.makedirs(output_folder, exist_ok=True)

    # Conectar a SQLite
    cursor = conn.cursor()

    # Obtener todas las tablas de SQLite
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()

    # Exportar cada tabla a un archivo CSV
    for table in tables:
        table_name = table[0]
        print(f"\nExportando tabla: {table_name}...")

        # Leer la tabla en un DataFrame
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
        print(f"Cantidad de columnas en la tabla {table_name}: {len(df.columns)}")

        # Guardar como CSV
        output_file = os.path.join(output_folder, f"{table_name}.csv")
        df.to_csv(output_file, index=False, encoding="utf-8-sig")

        print(f"✅ Tabla {table_name} exportada a {output_file}")

    print("🎉 Exportación completada.")

