from user_agents import user_agents
import random
from requests_html import HTMLSession


def get_session() -> HTMLSession:
    session = HTMLSession()
    #session.proxies = {'http': 'rproxy:5566',
    #                   'https': 'rproxy:5566'}
    session.headers = get_headers()
    return session


def get_headers() -> dict:
    return {
        'Host': 'www.funda.nl',
        'Connection': 'keep-alive',
        'User-Agent': f'{random.choice(user_agents)}',
        'Accept': "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'en-US,en;q=0.9'
    }