import decimal as D
import logging

import pandas as pd
import pyarrow as pa
from flytekit import FlyteContext, StructuredDatasetType, kwtypes, task, workflow
from flytekit.types.structured.structured_dataset import StructuredDataset
from pyarrow import csv, fs
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split
from sklearn.svm import SVC
from typing_extensions import Annotated

titanic_columns = kwtypes(
    pclass=int,
    survived=bool,
    name=str,
    sex=str,
    age=int,
    sibsp=str,
    parch=str,
    ticket=str,
    fare=float,  # no Decimal type yet :O
    embarked=str,
    boat=str,
    body=str,
    cabin=str,  # D.Decimal is not supported :O
)


@task
def extract_titanic_dataset(
    uri: str = "amazon-sagemaker-data-wrangler-documentation-artifacts/walkthrough_titanic.csv",
) -> Annotated[StructuredDataset, titanic_columns]:
    s3 = fs.S3FileSystem(region="us-west-2")
    with s3.open_input_file(uri) as csv_file:
        df = pd.read_csv(csv_file)
    return StructuredDataset(dataframe=df)


transformed_dataset_columns = titanic_columns


@task
def transform_titanic_dataset(
    df: StructuredDataset,
) -> Annotated[StructuredDataset, transformed_dataset_columns]:

    df = df.open(pd.DataFrame).all()  # open df
    # fill misisng embarked fields
    df.loc[lambda df: df["embarked"].isna(), "embarked"] = "S"
    return StructuredDataset(dataframe=df)


@task
def load_to_storage(df: StructuredDataset):
    logging.info(f"side-effects to database with dataframe: {df.metadata}")
    return 0


@workflow
def extract_transform_load_wf(uri: str):
    df = extract_titanic_dataset(uri=uri)
    df_t = transform_titanic_dataset(df=df)
    load_to_storage(df=df_t)


if __name__ == "__main__":
    acc = extract_transform_load_wf(
        uri="amazon-sagemaker-data-wrangler-documentation-artifacts/walkthrough_titanic.csv"
    )
    print(f"Our accuracy is: {acc}")
