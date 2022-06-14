import pysftp

class SftpConnector():

    def __init__(self, host, port, user='', passwd='', acct='', timeout=None) -> None:
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        self.acct = acct
        self.timeout = timeout

    # connect by creating an SFTP object
    def connect(self):
        try:
            self.connection = pysftp.Connection(host=self.host, port=self.port, username=self.user, password=self.passwd)
            print("Connection successfully established ... ")
        except:
            print('Failed to establish connection to target server')

    # list elements of a directory
    def list_dir(self, dir):
        try:
            self.connection.cwd(dir)
            directory_structure = self.connection.listdir_attr()
            for attr in directory_structure:
                print(attr.filename, attr)

            return directory_structure
        except AttributeError:
            return "Missing attribute (probably 'connection'), please connect to the server again!"

    # download a file by specifying a path to it on the server
    def download(self, filename, local_filename):
        try:
            self.connection.get(filename, local_filename)
        except AttributeError:
            return "Missing attribute (probably 'connection'), please connect to the server again!"
