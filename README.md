[<< MLPSuite home](..)
## MLPSuite Engine

Provides easy interface to build Pyspark ML pipelines from yaml file.

## Submitting to Spark
### main file
Engine exposes interface `interface.py` -> `Interface(path_to_config)`

Your `main.py` file for `spark-submit` should look like following:
```
from mlpsuite_engine.interface import Interface

interface = Interface(<path to yaml config>)

if __name__ == "__main__":
    # To run train:
    interface.run_train() 
```
If you want to run predictions: `interface.run_predict()`

### Submitting dependencies
Released zip version of engine must be added to spark-submit as `--py-files <path to dependencies.zip>`

Engine uses kafka for input and output. Find correct spark-sql-kafka package on maven: https://mvnrepository.com/ and use it in spark submit as `--jars <path to kafka jar> --packages <package Maven coordinates>`


### Configuration:
Configuration yaml for the **mlpsuite_engine** consists of 3 main sections: `stages`, `train` and `predict`.

Section `stages` creates all the stages for the pyspark ml pipeline (https://spark.apache.org/docs/latest/ml-pipeline.html). 
You need to specify module and class. 
You can specify any Transformer or Estimator, even custom ones. Ordering is important.

Section `train` describes input data and output location.

In `predict` section, you specify pipeline which was trained by **train** process. Also, kafka input and output is specified here.

This is an example of engine configuration:
```
stages:
  - tokenizer:
    - module_name: pyspark.ml.feature
    - class_name: Tokenizer
    - args:
      - inputCol=text[str]
      - outputCol=words[str]
  - hashingtf:
    - module_name: pyspark.ml.feature
    - class_name: HashingTF
    - args:
      - inputCol=words[str]
      - outputCol=features[str]
  - logistic_reg:
    - module_name: pyspark.ml.classification
    - class_name: LogisticRegression
    - args: 
      - maxIter=10[int]
      - regParam=0.001[float]
---
train:
  - path: /shared/train_test.csv
  - header: true
  - schema:
    - id: IntegerType
    - text: StringType
    - label: DoubleType
  - pipeline_output: /shared/models/
---
predict:
  - pipeline_path: /shared/models/
  - version: 202301251941_RTE
  - schema:
    - id: IntegerType
    - text: StringType

  - input_kafka_options:
      - kafka.bootstrap.servers: "kafka:9092"
      - subscribe: "input"
      - failOnDataLoss: "false"

  - output_kafka_options:
      - kafka.bootstrap.servers: "kafka:9092"
      - checkpointLocation: "/shared/checkpoint"
      - topic: "output"
```

In this example we specified stages, each with custom arguments specified in `args`. 
Training part takes `train_test.csv` and saves trained pipeline in `/shared/models/`.
During prediction, pipeline model from correct path is loaded with version `202301251941_RTE`.
Streaming into the model is done with `kafka:9092` on topic `input`. 
Predicted dataframe (transformed by the pipeline) is published to the same kafka into topic `output`.
### Development:
In order to build new zip with dependencies you can use helper batch script: `create_dependencies.bat`

