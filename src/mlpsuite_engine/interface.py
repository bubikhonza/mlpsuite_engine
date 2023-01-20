from pyspark.ml import Pipeline, PipelineModel
from pyspark.sql import SparkSession
from collections import ChainMap
from mlpsuite_engine.stage import Stage
from mlpsuite_engine.data_loader import DataLoader
from pyspark.sql import DataFrame
from pyspark.sql.streaming import DataStreamWriter, DataStreamReader
import pyspark.sql.functions as F
from datetime import datetime
import yaml
import logging
import os
import glob
import random
import string


class Interface:
    DT_FORMAT = "%Y%m%d%H%M_"

    def __init__(self, config_path: str):
        self.__config = DataLoader.load_yml(config_path)
        self.__stages_conf = self.__config["stages"]
        self.__train_conf = dict(ChainMap(*self.__config["train"]))
        self.__predict_conf = dict(ChainMap(*self.__config["predict"]))
        self.__train_data_schema = dict(ChainMap(*self.__train_conf["schema"]))
        self.__predict_data_schema = dict(
            ChainMap(*self.__predict_conf["schema"]))
        self.__input_kafka_settings = dict(
            ChainMap(*self.__predict_conf["input_kafka_options"]))
        self.__output_kafka_settings = dict(
            ChainMap(*self.__predict_conf["output_kafka_options"]))

    def __create_pipeline(self) -> Pipeline:
        pyspark_stages = []
        for s_d in self.__stages_conf:
            stage_attributes = dict(ChainMap(*next(iter(s_d.values()))))
            stage = Stage(**stage_attributes)
            pyspark_stages.append(stage.construct_pyspark_obj())

        return Pipeline(stages=pyspark_stages)

    def __create_pyspark_train_schema(self):
        from pyspark.sql.types import StringType, StructField, StructType, TimestampType, DoubleType, IntegerType
        schema = StructType()
        for k, v in self.__train_data_schema.items():
            schema.add(StructField(k, eval(v)(), nullable=True))
        return schema

    def __create_pyspark_predict_schema(self):
        from pyspark.sql.types import StringType, StructField, StructType, TimestampType, DoubleType, IntegerType
        schema = StructType()
        for k, v in self.__predict_data_schema.items():
            schema.add(StructField(k, eval(v)(), nullable=True))
        return schema

    def __load_train_data(self) -> DataFrame:
        path = self.__train_conf["path"]
        header = self.__train_conf["header"]
        schema = self.__create_pyspark_train_schema()
        return DataLoader.load_csv(path, header, schema)

    def __build_kafka_input(self, spark: SparkSession) -> DataFrame:
        logger = logging.getLogger('pyspark')
        input_stream_df = spark \
            .readStream \
            .format("kafka")
        for k, v in self.__input_kafka_settings.items():
            logger.error(str(k))
            logger.error(str(v))
            input_stream_df.option(k, v)
        return input_stream_df.load()

    def __build_kafka_output(self, spark: SparkSession, result: DataFrame) -> DataStreamWriter:
        query = result \
            .writeStream \
            .format("kafka")

        for k, v in self.__output_kafka_settings.items():
            query.option(k, v)
        return query.start()

    def __load_pipeline(self, version: str) -> PipelineModel:
        if version:
            model_path = os.path.join(
                self.__predict_conf["pipeline_path"], version)
        else:
            root_dir = self.__predict_conf["pipeline_path"]
            latestdir = max([os.path.join(root_dir, d)
                            for d in os.listdir(root_dir)], key=os.path.getmtime)
            model_path = os.path.join(
                self.__predict_conf["pipeline_path"], latestdir)
        return PipelineModel.load(model_path)

    def run_train(self) -> None:
        spark = SparkSession.builder.appName("Pipeliner").getOrCreate()

        pipeline = self.__create_pipeline()
        fitted_pipeline = pipeline.fit(self.__load_train_data())
        output_path = os.path.join(self.__train_conf["pipeline_output"],
                                   str(datetime.now().strftime(Interface.DT_FORMAT)) + ''.join(
                                       random.choices(string.ascii_uppercase + string.digits, k=3)))
        fitted_pipeline.save(output_path)
        spark.stop()

    def run_predict(self) -> None:
        logger = logging.getLogger('pyspark')
        logger.error("My test info statement")
        spark = SparkSession.builder.appName("Pipeliner").getOrCreate()
        version = str(self.__predict_conf.get("version", ""))
        pipeline = self.__load_pipeline(version)

        input_stream_df = self.__build_kafka_input(spark)

        schema = self.__create_pyspark_predict_schema()
        df = input_stream_df.selectExpr("CAST(value as STRING)")
        df = df.select(
            F.from_json(F.col("value"), schema).alias("sample")
        )
        df = df.select("sample.*")
        result = pipeline.transform(df)
        result = result.select([F.col(c).cast("string")
                               for c in result.columns])
        result = result.withColumn(
            "value", F.to_json(F.struct("*")).cast("string"), )
        query = self.__build_kafka_output(spark, result)

        query.awaitTermination()
