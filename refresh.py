import requests


def refresh():
    requests.get("https://fontaine.onrender.com")


if __name__ == "__main__":
    refresh()
