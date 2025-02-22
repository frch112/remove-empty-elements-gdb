import arcpy
import os

# Obtener la ruta de la geodatabase como parámetro
gdb_path = arcpy.GetParameterAsText(0)

try:
    # Verificar que la geodatabase existe
    if not arcpy.Exists(gdb_path):
        arcpy.AddError(f"No se puede acceder a la geodatabase: {gdb_path}")
        arcpy.AddError("Por favor, verifica la ruta proporcionada.")
        exit()

    # Inicializar contadores para el seguimiento de todas las operaciones
    contadores = {
        'datasets': 0,
        'datasets_vacios': 0,
        'datasets_eliminados': 0,
        'feature_classes': 0,
        'tablas': 0,
        'elementos_vacios': 0,
        'eliminados': 0
    }
    
    # Lista para almacenar todos los elementos eliminados
    elementos_eliminados = []
    # Lista para realizar un seguimiento de los datasets procesados
    datasets_procesados = []

    # Establecer el workspace inicial
    arcpy.env.workspace = gdb_path
    arcpy.AddMessage(f"\nProcesando geodatabase: {gdb_path}")

    # FASE 1: Procesar y eliminar feature classes vacíos dentro de datasets
    arcpy.AddMessage("\nFASE 1: Buscando y eliminando feature classes vacíos...")
    datasets = arcpy.ListDatasets("", "Feature")
    
    if datasets:
        arcpy.AddMessage(f"Se encontraron {len(datasets)} datasets")
        
        # Procesar cada dataset
        for ds in datasets:
            contadores['datasets'] += 1
            datasets_procesados.append(ds)  # Registrar el dataset para verificación posterior
            ds_path = os.path.join(gdb_path, ds)
            arcpy.AddMessage(f"\nRevisando dataset: {ds}")
            
            # Cambiar workspace al dataset actual
            arcpy.env.workspace = ds_path
            
            # Procesar feature classes dentro del dataset
            for fc in arcpy.ListFeatureClasses():
                contadores['feature_classes'] += 1
                fc_path = os.path.join(ds_path, fc)
                arcpy.AddMessage(f"  Revisando feature class: {fc}")
                
                try:
                    with arcpy.da.SearchCursor(fc_path, ['OID@']) as cursor:
                        if next(cursor, None) is None:
                            contadores['elementos_vacios'] += 1
                            arcpy.AddMessage(f"    El feature class está vacío: {fc}")
                            arcpy.Delete_management(fc_path)
                            elementos_eliminados.append(f"Feature Class: {ds}/{fc}")
                            contadores['eliminados'] += 1
                            arcpy.AddMessage(f"    Elemento eliminado exitosamente")
                        else:
                            arcpy.AddMessage(f"    El feature class contiene datos")
                except Exception as e:
                    arcpy.AddWarning(f"    Error al procesar {fc}: {str(e)}")
    else:
        arcpy.AddMessage("No se encontraron datasets")

    # Restablecer workspace a la raíz
    arcpy.env.workspace = gdb_path

    # Procesar feature classes en la raíz
    arcpy.AddMessage("\nBuscando feature classes en la raíz...")
    for fc in arcpy.ListFeatureClasses():
        contadores['feature_classes'] += 1
        fc_path = os.path.join(gdb_path, fc)
        arcpy.AddMessage(f"Revisando feature class: {fc}")
        
        try:
            with arcpy.da.SearchCursor(fc_path, ['OID@']) as cursor:
                if next(cursor, None) is None:
                    contadores['elementos_vacios'] += 1
                    arcpy.AddMessage(f"  El feature class está vacío: {fc}")
                    arcpy.Delete_management(fc_path)
                    elementos_eliminados.append(f"Feature Class: {fc}")
                    contadores['eliminados'] += 1
                    arcpy.AddMessage(f"  Elemento eliminado exitosamente")
                else:
                    arcpy.AddMessage(f"  El feature class contiene datos")
        except Exception as e:
            arcpy.AddWarning(f"  Error al procesar {fc}: {str(e)}")

    # Procesar tablas
    arcpy.AddMessage("\nBuscando tablas...")
    for tabla in arcpy.ListTables():
        contadores['tablas'] += 1
        tabla_path = os.path.join(gdb_path, tabla)
        arcpy.AddMessage(f"Revisando tabla: {tabla}")
        
        try:
            with arcpy.da.SearchCursor(tabla_path, ['OID@']) as cursor:
                if next(cursor, None) is None:
                    contadores['elementos_vacios'] += 1
                    arcpy.AddMessage(f"  La tabla está vacía: {tabla}")
                    arcpy.Delete_management(tabla_path)
                    elementos_eliminados.append(f"Tabla: {tabla}")
                    contadores['eliminados'] += 1
                    arcpy.AddMessage(f"  Tabla eliminada exitosamente")
                else:
                    arcpy.AddMessage(f"  La tabla contiene datos")
        except Exception as e:
            arcpy.AddWarning(f"  Error al procesar {tabla}: {str(e)}")

    # FASE 2: Verificar y eliminar datasets vacíos
    arcpy.AddMessage("\nFASE 2: Verificando datasets vacíos...")
    arcpy.env.workspace = gdb_path
    
    # Verificar cada dataset procesado
    for ds in datasets_procesados:
        ds_path = os.path.join(gdb_path, ds)
        arcpy.AddMessage(f"\nVerificando si el dataset está vacío: {ds}")
        
        try:
            # Cambiar al dataset para listar sus elementos
            arcpy.env.workspace = ds_path
            
            # Contar todos los tipos de elementos en el dataset
            fcs = arcpy.ListFeatureClasses()
            
            # Si no hay feature classes, el dataset está vacío
            if not fcs:
                contadores['datasets_vacios'] += 1
                arcpy.AddMessage(f"  El dataset está vacío: {ds}")
                
                # Cambiar workspace a la raíz antes de eliminar
                arcpy.env.workspace = gdb_path
                
                try:
                    arcpy.Delete_management(ds_path)
                    contadores['datasets_eliminados'] += 1
                    elementos_eliminados.append(f"Dataset: {ds}")
                    arcpy.AddMessage(f"  Dataset eliminado exitosamente")
                except Exception as e:
                    arcpy.AddError(f"  Error al eliminar dataset {ds}: {str(e)}")
            else:
                arcpy.AddMessage(f"  El dataset contiene {len(fcs)} feature classes")
                
        except Exception as e:
            arcpy.AddWarning(f"Error al verificar dataset {ds}: {str(e)}")

    # Restablecer workspace final
    arcpy.env.workspace = gdb_path

    # Mostrar resumen final con todas las operaciones
    arcpy.AddMessage("\n=== RESUMEN DE OPERACIONES ===")
    arcpy.AddMessage(f"""
Estadísticas del proceso:
- Datasets procesados: {contadores['datasets']}
- Datasets vacíos encontrados: {contadores['datasets_vacios']}
- Datasets eliminados: {contadores['datasets_eliminados']}
- Feature classes revisados: {contadores['feature_classes']}
- Tablas revisadas: {contadores['tablas']}
- Elementos vacíos encontrados: {contadores['elementos_vacios']}
- Elementos eliminados exitosamente: {contadores['eliminados']}
""")

    if elementos_eliminados:
        arcpy.AddMessage("\nElementos eliminados:")
        for elemento in elementos_eliminados:
            arcpy.AddMessage(f"- {elemento}")
    else:
        arcpy.AddMessage("\nNo se encontraron elementos vacíos para eliminar.")

except arcpy.ExecuteError:
    arcpy.AddError(arcpy.GetMessages(2))
except Exception as e:
    arcpy.AddError(f"Error inesperado: {str(e)}")