from trading import *


def test_get_markets() -> None:
    sites = [Site.NY]
    result = get_markets_for_sites(sites)
    for k, v in result.items():
        assert isinstance(k, EventTicker)
        for ii in v:
            assert isinstance(ii, MarketTicker)
    sites = []
    result = get_markets_for_sites(sites)
    assert True
    sites = all_sites()
    result = get_markets_for_sites(sites)
    for k, v in result.items():
        assert isinstance(k, EventTicker)
        for ii in v:
            assert isinstance(ii, MarketTicker)


def test_all_sites() -> None:
    sites = all_sites()
    true_sites = ["NY", "CHI", "MIA", "AUS", "DEN", "PHIL", "LAX"]
    for i in range(len(sites)):
        assert sites[i].value == true_sites[i]
