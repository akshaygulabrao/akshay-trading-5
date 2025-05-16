import numpy as np
import math


def odds2(odds):
    assert isinstance(odds,float)
    away_prob = (-odds) / (-odds + 100) if odds < 0 else 100 / (odds + 100)
    assert isinstance(away_prob,float)
    return away_prob
