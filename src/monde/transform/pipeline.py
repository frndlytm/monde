import pandas as pd
import pandera as pa

from monde import utils
from monde.transform import abstract, mixins, protected, simple
from monde.transform.validator import Validator

__all__ = ["EasyPreprocess", "EasyValidate", "Pipeline"]


class Pipeline(abstract.Transform):
    """TransformerPipeline enables a sequence of transform Estimators ending
    in a transform.

    According to the sklearn.pipeline.Pipeline docs, the final component of a
    Pipeline must implement fit; so this motivates a TransformerPipeline.

        https://scikit-learn.org/stable/modules/generated/
            sklearn.pipeline.Pipeline.html#sklearn.pipeline.Pipeline

    Params

        :type steps: tuple[tuple[str, TransformerMixin]]
        :param steps:
            A *-unpacked sequence of named pipeline steps subclassing the
            `(BaseEstimator, TransformerMixin)` classes, i.e. implements
            either `transform` or `fit_transform`.

    Usage

        >>> # Create a schema to use the example steps...
        >>> schema = pa.DataFrameSchema(...)
        >>> pipeline = transforms.Pipeline(
        ...     ("extract_subframe", SubframeExtractor()),
        ...     ("optimize_memory", MemoryOptimizer(schema)),
        ...     ("validate", Validator(schema)),
        ... )
        >>>
        >>> # Pipe the dataframe through the transform
        >>> X = pd.read_excel("s3://path/to/workbook.xlsx").pipe(pipeline)

    """

    def __init__(self, *steps: tuple[str, abstract.Transform]):
        self.steps = dict([("start", simple.Identity()), *steps])

    def add_step(self, key: str, step: abstract.Transform) -> "Pipeline":
        self.steps.update({key: step})
        return self

    @utils.timed
    def fit_transform(self, X: pd.DataFrame, y=None, **fit_params) -> pd.DataFrame:
        # Run the steps in sequence
        for step in self.steps.values():
            # Try to manipulate X inplace with fit_transform first.
            try:
                X = X.pipe(step.fit_transform, y=y, **fit_params)  # type: ignore

            # Back-off to step.transform if not exists.
            except NotImplementedError:
                #  NOTE: may also raise NotImplementedError
                X = X.pipe(step.transform)  # type: ignore

        return X


class EasyPreprocess(Pipeline, mixins.SchemaDriven):
    """
    EasyPreprocess is a pipeline that strings together all of the simple.Clean
    transforms. Using the schema, it detects string columns that should be a
    different type and coerces them.
    """

    def __init__(self, schema: pa.DataFrameSchema):
        mixins.SchemaDriven.__init__(self, schema)
        Pipeline.__init__(
            self,
            *(
                ("clean_strings", simple.CleanStrings(schema)),
                ("clean_booleans", simple.CleanBooleans(schema)),
                ("clean_integers", simple.CleanIntegers(schema)),
                ("clean_floats", simple.CleanFloats(schema)),
            ),
        )


class EasyValidate(Pipeline, mixins.SchemaDriven):
    """
    EasyValidate is a pipeline that RenameAliases, EasyPreprocesses and Validates
    the data, with optional HashProtectedAttributes.
    """

    def __init__(self, schema: pa.DataFrameSchema, protect: bool = False, **kwargs):
        mixins.SchemaDriven.__init__(self, schema)
        Pipeline.__init__(
            self,
            *(
                ("rename", simple.RenameAliases(schema)),
                ("preprocess", EasyPreprocess(schema)),
                # ("optimize", MemoryOptimizer(schema)),
                ("validate", Validator(schema, **kwargs)),
            ),
        )
        # fmt:off
        if protect: self.add_step("protect", protected.HashProtectedAttributes(schema))
        # fmt:on
