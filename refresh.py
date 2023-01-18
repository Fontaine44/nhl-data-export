import requests


def refresh():
    requests.get("https://fontaine.onrender.com/start")


if __name__ == "__main__":
    refresh()
