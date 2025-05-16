import numpy as np
import math


def odds2(away_odds,home_odds):
    # Calculate implied probabilities (without assuming favorite/underdog order)
    home_prob = (-home_odds) / (-home_odds + 100) if home_odds < 0 else 100 / (home_odds + 100)
    away_prob = (-away_odds) / (-away_odds + 100) if away_odds < 0 else 100 / (away_odds + 100)

    # Remove vig (normalize to 100%)
    total_prob = home_prob + away_prob
    adjusted_home = home_prob / total_prob
    adjusted_away = away_prob / total_prob

    return adjusted_away,adjusted_home # Always returns (home, away)


{'clientMetadata': {'feature': 'event', 'X-Client-Name': 'web', 'X-Client-Version': '2519.1.1.7'}, 'jwt': 'eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImN0eSI6IkpXVCJ9.eyJJc0Fub255bW91cyI6IlRydWUiLCJTaXRlR3JvdXBJZCI6IjM5IiwiU2l0ZUlkIjoiMTUwNjUiLCJEb21haW5JZCI6IjQ0OSIsIkNvdW50cnlDb2RlIjoiVVMiLCJuYmYiOjE3NDcxNzQxMDgsImV4cCI6MTc0NzE3NDIyOCwiaWF0IjoxNzQ3MTc0MTA4LCJpc3MiOiJ1cm46ZGsiLCJhdWQiOiJ1cm46ZGsifQ.lWXiVhONYSesRpapFrl85wSYtBmLKYrXYtT0ItDnNeOkGGVil2rlNV1F2_ag3hER_IPORzbZBybfF_UU4vv1HVGbSdQezzNm159Vq908Osk7R_dWvmU7OcKMTYsMaCKpHhyHhCIZwsNdfPp_pcPlRx4NNloUIq1CDOlKTZ97bqBy8Eobd9QZfw_2ULdYrwmjGnrMIvhyeZkMFllWtYHg9Gi74Pa1aiqgbx4v2x4kYZzChxb4SZ-XHfTgGM7xLBWsGCzIXW86_Dt0JE5dA7i7MYvHkjOjJoeWQq3LJFIxbCzdLKg1M0jB6ZN4XAbOIxg874XXC6bBvzWr9AHf63NgYisvzAZiSRTEUVvuzNcUmiqW63W9gtOYxKK2sPk3AmU2ttOTlUppM-JH3X19lhNp9pPV6v0AJ4I0RwpsDUWuiyAsWJ2Gnst9ODBbSVLq_G49F6DPMTvzPMBNy_CaH77I38l9rGjn05uSNrZmGB_fkeDiclGrUbMmWZvjQnckjAXY7o8Qmu1wUofm_TKVceULYx5XOLyiZN9yt0Z49r98ZyqqImOr6bL0RBmLMlqR-QVFc537_WqCLTrByqdTMKBqibm1lapw0F8lZWBSj1jCnRk3bk-iTYsMoVZtFjLjXwriItp6o1yFRzWs5WXOQTU0916v1Hi2Q4dmsZSESBX6KSo', 'siteName': 'dkusnj'}