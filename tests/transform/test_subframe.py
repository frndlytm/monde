import pandas as pd

from monde.transform import SubframeExtractor


def test_SubframeExtractor(data):
    X = pd.read_excel(data.joinpath("has_subframes.xlsx"))
    check = X.pipe(SubframeExtractor(header=True).fit_transform)
    assert check.size == 100
    assert check.to_numpy().max() == 81
