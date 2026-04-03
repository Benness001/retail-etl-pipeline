import requests
import json

def extract():
    print("Starting extraction from Retail API...")

    api_url = "https://jsonplaceholder.typicode.com/users"
    output_path = "/tmp/raw_data.json"

    print("Environment ready")
    print(f"api url: {api_url}")
    print(f"output path: {output_path}")

    response = requests.get(api_url)
    response.raise_for_status()

    data = response.json()

    with open(output_path, "w") as f:
        json.dump(data, f, indent=4)

    print(f"Extraction complete. {len(data)} records saved to {output_path}")
    return output_path


