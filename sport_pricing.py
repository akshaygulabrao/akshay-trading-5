import numpy as np
import math


def odds2(odds):
    away_prob = (-odds) / (-odds + 100) if odds < 0 else 100 / (odds + 100)
    return away_prob
