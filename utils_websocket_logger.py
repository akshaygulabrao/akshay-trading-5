import logging
import msgpack
from mitmproxy import http

def websocket_message(flow: http.HTTPFlow):
    assert flow.websocket is not None  # make type checker happy
    message = flow.websocket.messages[-1]

    try:
        decoded = msgpack.unpackb(message.content, raw=False)  # Keep strings as bytes
        if isinstance(decoded,dict):
            for k,v in decoded.items():
                print(k)
                print(v)
                print('---')
            print(decoded)
        if isinstance(decoded,list):
            if decoded[0] == '83feb67b-40b8-40d4-82fb-cb7b3cbc05d4':
                print(decoded)
                print('\n\n')
    except Exception as e:
        logging.info(f"{message.content!r}")

subscribe_msg = {'jsonrpc': '2.0', 'params': {'entity': 'events', 'queryParams': {'query': "$filter=leagueId eq '84240'", 'initialData': False, 'includeMarkets': 'none', 'projection': 'sportsbook', 'locale': 'en-US'}, 'forwardedHeaders': {}, 'clientMetadata': {'feature': 'event', 'X-Client-Name': 'web', 'X-Client-Version': '2519.1.1.7'}, 'jwt': 'eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImN0eSI6IkpXVCJ9.eyJJc0Fub255bW91cyI6IlRydWUiLCJTaXRlR3JvdXBJZCI6IjM5IiwiU2l0ZUlkIjoiMTUwNjUiLCJEb21haW5JZCI6IjQ0OSIsIkNvdW50cnlDb2RlIjoiVVMiLCJuYmYiOjE3NDcxNzQxMDgsImV4cCI6MTc0NzE3NDIyOCwiaWF0IjoxNzQ3MTc0MTA4LCJpc3MiOiJ1cm46ZGsiLCJhdWQiOiJ1cm46ZGsifQ.lWXiVhONYSesRpapFrl85wSYtBmLKYrXYtT0ItDnNeOkGGVil2rlNV1F2_ag3hER_IPORzbZBybfF_UU4vv1HVGbSdQezzNm159Vq908Osk7R_dWvmU7OcKMTYsMaCKpHhyHhCIZwsNdfPp_pcPlRx4NNloUIq1CDOlKTZ97bqBy8Eobd9QZfw_2ULdYrwmjGnrMIvhyeZkMFllWtYHg9Gi74Pa1aiqgbx4v2x4kYZzChxb4SZ-XHfTgGM7xLBWsGCzIXW86_Dt0JE5dA7i7MYvHkjOjJoeWQq3LJFIxbCzdLKg1M0jB6ZN4XAbOIxg874XXC6bBvzWr9AHf63NgYisvzAZiSRTEUVvuzNcUmiqW63W9gtOYxKK2sPk3AmU2ttOTlUppM-JH3X19lhNp9pPV6v0AJ4I0RwpsDUWuiyAsWJ2Gnst9ODBbSVLq_G49F6DPMTvzPMBNy_CaH77I38l9rGjn05uSNrZmGB_fkeDiclGrUbMmWZvjQnckjAXY7o8Qmu1wUofm_TKVceULYx5XOLyiZN9yt0Z49r98ZyqqImOr6bL0RBmLMlqR-QVFc537_WqCLTrByqdTMKBqibm1lapw0F8lZWBSj1jCnRk3bk-iTYsMoVZtFjLjXwriItp6o1yFRzWs5WXOQTU0916v1Hi2Q4dmsZSESBX6KSo', 'siteName': 'dkusnj'}, 'method': 'subscribe', 'id': '641f5097-b5b4-4529-b3ab-2703eab10e86'}

# jsonrpc
# 2.0
# ---
# params
# {'entity': 'events', 'queryParams': {'query': "$filter=leagueId eq '84240'", 'initialData': False, 'includeMarkets': 'none', 'projection': 'sportsbook', 'locale': 'en-US'}, 'forwardedHeaders': {}, 'clientMetadata': {'feature': 'event', 'X-Client-Name': 'web', 'X-Client-Version': '2519.1.1.7'}, 'jwt': 'eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImN0eSI6IkpXVCJ9.eyJJc0Fub255bW91cyI6IlRydWUiLCJTaXRlR3JvdXBJZCI6IjM5IiwiU2l0ZUlkIjoiMTUwNjUiLCJEb21haW5JZCI6IjQ0OSIsIkNvdW50cnlDb2RlIjoiVVMiLCJuYmYiOjE3NDcxNzQxMDgsImV4cCI6MTc0NzE3NDIyOCwiaWF0IjoxNzQ3MTc0MTA4LCJpc3MiOiJ1cm46ZGsiLCJhdWQiOiJ1cm46ZGsifQ.lWXiVhONYSesRpapFrl85wSYtBmLKYrXYtT0ItDnNeOkGGVil2rlNV1F2_ag3hER_IPORzbZBybfF_UU4vv1HVGbSdQezzNm159Vq908Osk7R_dWvmU7OcKMTYsMaCKpHhyHhCIZwsNdfPp_pcPlRx4NNloUIq1CDOlKTZ97bqBy8Eobd9QZfw_2ULdYrwmjGnrMIvhyeZkMFllWtYHg9Gi74Pa1aiqgbx4v2x4kYZzChxb4SZ-XHfTgGM7xLBWsGCzIXW86_Dt0JE5dA7i7MYvHkjOjJoeWQq3LJFIxbCzdLKg1M0jB6ZN4XAbOIxg874XXC6bBvzWr9AHf63NgYisvzAZiSRTEUVvuzNcUmiqW63W9gtOYxKK2sPk3AmU2ttOTlUppM-JH3X19lhNp9pPV6v0AJ4I0RwpsDUWuiyAsWJ2Gnst9ODBbSVLq_G49F6DPMTvzPMBNy_CaH77I38l9rGjn05uSNrZmGB_fkeDiclGrUbMmWZvjQnckjAXY7o8Qmu1wUofm_TKVceULYx5XOLyiZN9yt0Z49r98ZyqqImOr6bL0RBmLMlqR-QVFc537_WqCLTrByqdTMKBqibm1lapw0F8lZWBSj1jCnRk3bk-iTYsMoVZtFjLjXwriItp6o1yFRzWs5WXOQTU0916v1Hi2Q4dmsZSESBX6KSo', 'siteName': 'dkusnj'}
# ---
# method
# subscribe
# ---
# id
# 641f5097-b5b4-4529-b3ab-2703eab10e86,23fea858-d51e-426e-b35d-e789ab56e9e4
# ---