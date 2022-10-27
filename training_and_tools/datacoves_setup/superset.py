import requests
from bs4 import BeautifulSoup
import json
def get_supetset_session():
    """
    # https://superset-dev123.east-us-a.datacoves.com//swagger/v1
    url = f'http://{superset_host}/api/v1/chart/'
    r = s.get(url)
    # print(r.json())
    """
    superset_host = 'superset-dev123.east-us-a.datacoves.com'
    username = 'svc_datacoves'
    password = 'YOUR_PASSWORD'

    # set up session for auth
    s = requests.Session()
    login_form = s.post(f"http://{superset_host}/login")
    # get Cross-Site Request Forgery protection token
    soup = BeautifulSoup(login_form.text, 'html.parser')
    csrf_token = soup.find('input',{'id':'csrf_token'})['value']
    data = {
        'username': username,
        'password': password,
        'csrf_token':csrf_token
    }
    # login the given session
    s.post(f'http://{superset_host}/login/', data=data)
    print(dict(s.cookies))
    return s

base_url = 'superset-dev123.east-us-a.datacoves.com'

def get_dashboards_list(s, base_url=base_url):
    """## GET List of Dashboards"""

    url = base_url + '/api/v1/dashboard/'
    r = s.get(url)
    resp_dashboard = r.json()
    for result in resp_dashboard['result']:
        print(result['dashboard_title'], result['id'])

s = get_supetset_session()
# {'session': '.eJwlj8FqAzEMRP_F5z1Islay8jOLJcu0NDSwm5xK_r0uPQ7DG978lGOeeX2U2_N85VaOz1FuxVK6JIHu1QFhGuEOk5NG8qiYGkJ7rR3_Ym-uJMOzJqySeHhIG8SkNQK6GVhTdLf0ZMmG6sZGQtiQ1Gz0qYiUTVoHhohZthLXOY_n4yu_l0-VKTObLaE13i2Hz2A2rzBmhU7WkkN1cfdH9HsuZoFbeV15_l_C8v4F4nBC9A.Ypn16Q.yz4E-vz0gp3EmJwv-6tYIcOGavU'}
get_dashboards_list(s)
