from dotenv import load_dotenv
import os
import requests
import json

load_dotenv()


class CanalDirectoConection:
    def __init__(self, numero_identificacion):
        self.url = os.getenv("CANALDIRECTO_URL") + f"/{numero_identificacion}"
        self.headers = {
            "Content-type": "application/json",
            "Authorization": "Token " + os.getenv("CANALDIRECTO_TOKEN"),
        }
        self.data = {}

    def get(self):
        response = requests.get(self.url, headers=self.headers)
        return response.json()

    def post(self, path, data):
        response = requests.post(
            self.url + path, data=json.dumps(data), headers=self.headers
        )
        return response.json()

    def put(self, path, data):
        response = requests.put(
            self.url + path, data=json.dumps(data), headers=self.headers
        )
        return response.json()

