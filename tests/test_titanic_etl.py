import numpy as np
import pandas as pd
import pytest
from flytekit.types.structured.structured_dataset import StructuredDataset

from flyte_benchmarks import titanic_etl


def test_titanic_etl():
    df = titanic_etl.extract_titanic_dataset()

    assert df.open(pd.DataFrame).all().shape[0]


def test_titanic_transformation():
    df_test = StructuredDataset(pd.DataFrame({"embarked": ["A", "B", np.NaN]}))

    transformed = titanic_etl.transform_titanic_dataset(df=df_test)

    assert transformed.open(pd.DataFrame).all()["embarked"].tolist() == ["A", "B", "S"]
