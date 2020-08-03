from app import app

import requests
import json


def send_to_queue(payload):
    url = f'http://localhost:{app.config["PORT"]}/process'
    result = requests.post(url,
                           json=payload)
    if result.status_code == 200:
        print(f"""{payload}\nsent to\n{url}""")
    else:
        print(f"status code: {result.status_code}")
        print(f"""{json.dumps(payload)}\nsent to\n{url}""")
