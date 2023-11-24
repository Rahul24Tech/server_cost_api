from fastapi import FastAPI
import uvicorn
from fastapi.responses import JSONResponse
from small_server_cost import  get_all_small_n_big_server_cost

app = FastAPI()

set_key_small_server = '8a4405e8334dbc2fadcdcae7016088b5'

@app.get("/", response_class=JSONResponse)
def read_root():
    return {"Hello": "Hello to Tech3 Server Cost Fetcher API",
            "Get Server Cost Data": "Endpoint : /get_server_cost , Query Params: API key"}


@app.get("/get_server_cost", response_class=JSONResponse)
async def get_server_cost(api_key: str):
    try:
        print(api_key)
        if api_key == set_key_small_server :
            resp_data = get_all_small_n_big_server_cost()
            # print(resp_data)
            # big_data = resp_data[0]
            # small_data = resp_data[1]
            #
            #
            # # Add the new key-value pair right after "message"
            # new_key = "code"
            # new_value = "success"
            # big_data[new_key] = new_value
            #
            # # Reorganize the keys and create the new data dictionary
            # new_data = {
            #     "selected_date": big_data["selected_date"],
            #     "message": big_data["message"],
            #     new_key: new_value,
            #     "data": {key: big_data[key] for key in big_data if isinstance(key, int)}
            #
            # }
            #
            # # Add the new key-value pair right after "message"
            # new_key = "code"
            # new_value = "success"
            # big_data[new_key] = new_value
            #
            # # Reorganize the keys and create the new data dictionary
            # new_data_small = {
            #     "selected_date": small_data["selected_date"],
            #     "message": small_data["message"],
            #     new_key: new_value,
            #     "data": {key: small_data[key] for key in small_data if isinstance(key, int)}
            #
            # }
            # resp_data[0] = new_data
            # resp_data[1] = new_data_small

            return resp_data
        else:
            return {'Status': 'Error', 'error':'Invalid API Key'}

    except Exception as e:
        return {'Status': 'Error', 'error' :str(e)}

if __name__ == "__main__":
    uvicorn.run("main:app", host='0.0.0.0', port=15997, workers=1, reload=True)
