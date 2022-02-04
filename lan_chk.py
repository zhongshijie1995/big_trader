import requests
import re
import os
import subprocess

BLOG_PATH = r'D:\01-Project\zhongshijie1995.github.io'
ABOUT_FILE = r'source\about\index.md'
ABOUT_PATH = os.path.join(BLOG_PATH, ABOUT_FILE)


def get_about_ip() -> str:
    s = requests.get('https://zhongshijie1995.github.io/about/')
    return re.search(r'【.*】', s.content.decode()).group()[1: -1].strip()


def get_lan_ip() -> str:
    s = requests.get('https://ifconfig.me')
    return s.content.decode()


def update_about_ip() -> bool:
    ai = get_about_ip()
    li = get_lan_ip()
    if ai != li:
        with open(ABOUT_PATH, mode='r', encoding='utf-8') as f:
            content = f.read()
        content = content.replace(ai, li)
        with open(ABOUT_PATH, mode='w', encoding='utf-8') as f:
            f.write(content)
        return True
    return False


def deploy_blog():
    result = [
        subprocess.call('git pull', shell=True),
        subprocess.call('git add .', shell=True),
        subprocess.call('git commit -m "AutoCommit"', shell=True),
        subprocess.call('git push', shell=True),
        subprocess.call('npx hexo clean', shell=True),
        subprocess.call('npx hexo g', shell=True),
        subprocess.call('npx hexo deploy', shell=True),
        subprocess.call('npx hexo clean', shell=True)
    ]
    print(result)


if __name__ == '__main__':
    if update_about_ip():
        deploy_blog()
