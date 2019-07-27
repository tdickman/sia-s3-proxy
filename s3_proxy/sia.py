import requests

USER_AGENT = 'Sia-Agent'


class Sia(object):
    def __init__(self, host='127.0.0.1', port=9980, sia_password=None):
        self.host = host
        self.port = port
        self.s = requests.Session()
        self.s.auth = ("", sia_password)

    def _request(self, path, action='get', **kwargs):
        func = getattr(self.s, action)
        resp = func(
            f'http://{self.host}:{self.port}{path}',
            headers={'User-Agent': USER_AGENT},
            **kwargs,
        )

        if resp.status_code not in [200, 204]:
            raise Exception(f'Error: {resp.status_code} - {resp.text}')

        return resp

    def list(self, path):
        return self._request(f'/renter/dir/{path}').json()

    def create_folder(self, path):
        return self._request(
            f'/renter/dir/{path}/?action=create',
            action='post',
        )

    def delete_folder(self, path):
        return self._request(
            f'/renter/dir/{path}/?action=delete',
            action='post',
        )

    def upload_file(self, path, data):
        return self._request(
            f'/renter/uploadstream/{path}/?force=true',
            action='post',
            data=data,
        )

    def get_file_status(self, path):
        return self._request(f'/renter/file/{path}').json()['file']

    def get_file(self, path):
        return self.get_file_status(path), self._request(f'/renter/stream/{path}').content

    def delete_file(self, path):
        return self._request(
            f'/renter/delete/{path}',
            action='post'
        )


if __name__ == '__main__':
    s = Sia()
    print(s.list(''))
