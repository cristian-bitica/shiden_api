from pyspark.sql import SparkSession

from shiden.config.settings import Environment, settings


def get_spark() -> SparkSession:
    """
    Return an env-aware SparkSession.

    - LOCAL:      starts a local Spark instance with Delta Lake JARs and extensions
                  configured via configure_spark_with_delta_pip.  This resolves the
                  delta-spark JARs from the installed Python package so they are on
                  the JVM classpath — without it DeltaTable JVM calls fail with
                  'JavaPackage object is not callable'.
    - DATABRICKS: getOrCreate() returns the pre-configured cluster session where
                  Delta is already on the classpath; configure_spark_with_delta_pip
                  must NOT be called (it would conflict with the cluster config).
    """
    builder = (
        SparkSession.builder.appName(settings.spark_app_name)
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    if settings.environment == Environment.LOCAL:
        builder = (
            builder.master(settings.spark_master)
            .config("spark.driver.memory", "2g")
            .config("spark.sql.shuffle.partitions", "4")  # keep low for local dev
            .config("spark.ui.enabled", "false")
        )
        from delta import configure_spark_with_delta_pip
        return configure_spark_with_delta_pip(builder).getOrCreate()

    return builder.getOrCreate()
