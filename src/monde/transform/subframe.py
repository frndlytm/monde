from typing import Hashable, Literal, Sequence, Union

import numpy as np
import pandas as pd
from skimage.measure import label, regionprops
from skimage.measure._regionprops import RegionProperties

from monde.transform import abstract

__all__ = ["SubframeExtractor"]

IndexLabel = Union[Hashable, Sequence[Hashable]]


class SubframeExtractor(abstract.Transform):
    """
    SubframeExtractor extracts the specified (or largest) contiguous sub-frame
    from DataFrame.

    This is particularly useful after reading from an Excel Sheet where there
    is a bunch of extra, useless data at the header and footer of the actual
    data.

    Params

        :type header: bool
        :param header:

            Does the subframe have a header row? (Defaults to True)

        :type index: int, optional
        :param index:

            The positional index of the desired sub-frame, defaults to None.
            If None, the SubframeExtractor fits the index by seeking the
            largest contiguous subframe in the given frame.

    Usage

        >>> extractor = SubframeExtractor(index=-1)
        >>> X = (
        ...     pd.read_excel("s3://path/to/workbook.xlsx")
        ...     .pipe(extractor.fit_transform)
        ... )

    """

    def __init__(
        self,
        region: int | None = None,
        header: int | Sequence[int] | None = None,
        index_col: IndexLabel | Literal[False] | None = None,
    ):
        self.region = region
        self.header = header
        self.index_col = index_col

        # Initialize coordinates of the top-left and bottom-right
        # corner of the table
        self.x1, self.y1 = None, None
        self.x2, self.y2 = None, None

        # Initialize the names as NULL, to be fit later
        self.names = None

    @staticmethod
    def get_largest_region(regions: list[RegionProperties]) -> int:
        # Greedily take the largest-area sub-table from X.
        largest_size = -float("inf")
        largest_i = -1

        for i, region in enumerate(regions):
            if region.area_bbox > largest_size:
                largest_size = region.area_bbox
                largest_i = i

        return largest_i

    def fit(self, X: pd.DataFrame, y=None, **fit_params) -> "SubframeExtractor":  # type: ignore
        # Use NULL masking and contiguous image search to
        # get bounding box regions. Take the last (or first).
        larr = label(np.array(X.notnull()).astype("int"))
        regions = regionprops(larr)

        # Take the configured region index, or the largest (if None)
        if self.region is None:
            self.region = self.get_largest_region(regions)

        # Unpack the bounding box
        region_star = regions[self.region]
        self.x1, self.y1, self.x2, self.y2 = region_star.bbox

        return self

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        # Get the table region from the data
        region = X.iloc[self.x1 : self.x2, self.y1 : self.y2]  # noqa: E203

        # If we have a header
        if self.header is not None:
            # Split the header line out from the data
            region.rename(columns=region.iloc[0], inplace=True)
            region.drop(region.index[0], inplace=True)

            # Set the index if configured
            if self.index_col is not None:
                region.set_index(keys=self.index_col, inplace=True)

        return region
