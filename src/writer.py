import os


def write_df_to_adls(df, file_name, container_name, storage_name):
    print(f"Ecriture du DataFrame {file_name}...")
    try:
        df.write.format("parquet").mode("overwrite").option(
            "spark.sql.parquet.enableVectorizedWriter", "false"
        ).save(
            f"abfss://{container_name}@{storage_name}.dfs.core.windows.net/{file_name}"
        )
        print(f"DataFrame {file_name} ecrit avec succes")
    except Exception as e:
        print(f"Erreur lors de l'ecriture du DataFrame {file_name}: {e}")
        raise
