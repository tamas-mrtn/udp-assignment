import requests


class ExtractECB:
    def __init__(self, flowref: str, keys: str = None, parameters: dict = None):
        self.flowref = flowref
        self.keys = keys
        self.parameters = parameters
        start = parameters['startPeriod'].replace('-', '')
        end = parameters['endPeriod'].replace('-', '')
        self.destination = f'/opt/airflow/volume/{keys.lower()}_{start}-{end}.csv'

    def get(self):
        # Build the URL for the web service
        url = f"https://sdw-wsrest.ecb.europa.eu/service/data/{self.flowref}/"
        url += f"{self.keys}" if self.keys else ""
        url += "?format=csvdata"
        print(url)

        # Send the request to the web service
        response = requests.get(url, params=self.parameters)
        if not response.ok:
            raise ValueError(f"Failed to retrieve data: {response.status_code} - {response.reason}")
        return response.text

    def write_csv(self, data):
        with open(self.destination, 'w', encoding='UTF8') as f:
            f.write(data)

    def main(self):
        response = self.get()
        self.write_csv(response)
        return self.destination
