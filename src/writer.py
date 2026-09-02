def write_df_to_adls(df, file_name, container_name, storage_name):
    print(f"Ecriture du DataFrame {file_name}")
    df.write.parquet(
        f"abfss://{container_name}@{storage_name}.dfs.core.windows.net/{file_name}"
    ).mode("overwrite")


# https://www.youtube.com/watch?v=09fb7dBwaIQ
