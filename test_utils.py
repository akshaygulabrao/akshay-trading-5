import utils


def test_get_markets():
    markets = utils.get_markets()
    assert len(markets) != 0
