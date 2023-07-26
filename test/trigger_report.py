from requests import get

r = get('http://localhost:5000/trigger_report').json()
print(r)
