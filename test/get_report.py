from requests import get

payload = {'report_id': '123'}

r = get('http://localhost:5000/get_report', params=payload).json()
print(r)
