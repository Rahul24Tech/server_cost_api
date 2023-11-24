import requests

url_home = 'https://sky3team.com:15997/'
url_revenue_summary = 'https://sky3team.com:15997/get_small_server_cost'


request_params = {"start_date" : "2023-08-04", "end_date": "2023-07-04", "api_key": "8a4405e8334dbc2fadcdcae7016088b5"}
headers = {'Accept': 'application/json'}

hello_msg = requests.get(url_home)
print('hello_msg', hello_msg.json())

# revenue_data_fetched = requests.get(url = url_revenue_summary, params = request_params, headers=headers)
# # print('revenue_data_fetched', revenue_data_fetched.json())
# print(f"Revenue_Data_Fetched: {revenue_data_fetched.json()}")
try:
    t_rev_fetched = requests.get(url=url_revenue_summary, params=request_params, headers=headers)
    if t_rev_fetched.status_code == 200:
        response_json = t_rev_fetched.json()
        print("API Response JSON:")
        print(response_json)
    else:
        print("API Request Failed. Status Code:", t_rev_fetched.status_code)
except Exception as e:
    print("An error occurred:", e)
