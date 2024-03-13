import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin


class Transform(BaseEstimator, TransformerMixin):
    """Syntactic sugar for inheriting both of the above super classes."""

    def fit(self, _: pd.DataFrame, **__) -> "Transform":
        return self

    def __call__(self, X: pd.DataFrame, **fit_params) -> pd.DataFrame:
        return self.fit_transform(X, None, **fit_params)
