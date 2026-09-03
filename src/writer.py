def write_df_to_adls(df, file_name, container_name, storage_name):
    print(f"Ecriture du DataFrame {file_name}")
    df.write.format("parquet").mode("overwrite").save(
        f"abfss://{container_name}@{storage_name}.dfs.core.windows.net/{file_name}"
    )
