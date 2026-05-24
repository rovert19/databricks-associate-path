from pyspark import pipelines as dp
from pyspark.sql import functions as F


# Bronze Stage
@dp.table(
    name="dbassociate.bronze.ordenes",
    comment= "Tabla bronze de ordenes del area de logistica",
    table_properties={"quality": "bronze"},
)
def bronze_orders():
    return (
        spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("inferSchema", "true")
            .option("header", "true")
            .option("cloudFiles.schemaLocation", "/Volumes/dbassociate/default/vol_landing/session_08/_schemas/orders")
            .load("/Volumes/dbassociate/default/vol_landing/session_08/ordenes")
            .withColumn("ingest_at", F.current_timestamp())
            .withColumn("source_file", F.col("_metadata.file_path"))
    )

@dp.table(
    name="dbassociate.silver.ordenes_clean",
    comment="Ingesta de tabla ordenes aplicando expectacions",
    table_properties={"quality": "bronze"},
)
@dp.expect_or_drop("monto_positivo", "monto > 0")
@dp.expect_or_drop("ciudades no nulas", "ciudad_origen IS NOT NULL and ciudad_destino IS NOT NULL")
@dp.expect("peso_razonable", "peso_kg < 10000 and peso_kg > 0")
def silver_orders():
    return (
        spark.readStream
            .table("dbassociate.bronze.ordenes")
            .select(
                "orden_id",
                "cliente_id",
                F.col("fecha").cast("date").alias("fecha"),
                "estado",
                F.col("monto").cast("decimal(12,2)").alias("monto"), 
                "ciudad_origen",
                "ciudad_destino",
                "transportista",
                F.col("peso_kg").cast("decimal(6,2)").alias("peso_kg")
            )
    ) 

@dp.materialized_view(
    name="dbassociate.gold.kpi_transportistas",
    comment="Vista materializada con métricas sobre ordenes entregadas por los transportistas",
    table_properties={"quality": "gold"},
)
def gold_kpi_transportistas():
    return (
        spark.read
            .table("dbassociate.silver.ordenes_clean")
            .filter("estado = 'Entregado'")
            .groupBy("transportista")
            .agg(
                F.count("orden_id").alias("total_ordenes"),
                F.sum("monto").alias("monto_total"),
                F.avg("peso_kg").alias("peso_promedio")
            )
)
