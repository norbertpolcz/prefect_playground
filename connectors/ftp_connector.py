from ftplib import FTP
import pandas as pd

class FtpConnector():

    # we only need host, user and passwd to connect to 'test.rebex.net'
    # the parameters with = '' are optional parameters
    def __init__(self, host, user='', passwd='', acct='', timeout=None) -> None:
        self.host = host
        self.user = user
        self.passwd = passwd
        self.acct = acct
        self.timeout = timeout

    # connect by creating an FTP object
    def connect(self):
        self.ftp = FTP(self.host, self.user, self.passwd, self.acct, self.timeout)
        
    # list elements of a directory
    def list_dir(self, dir = ''):
        return self.ftp.retrlines(f'LIST {dir}')

    # download a file by specifying a path to it on the server
    def download(self, filename, local_filename):

        # open with 'x' creates the file if it does not exist yet, but throws error if the file already exists
        try:
            create_file = open(local_filename, 'x')
            create_file.close()
            open_to_write = open(local_filename, 'wb')
        except:
            open_to_write = open(local_filename, 'wb')

        self.ftp.retrbinary(f'RETR {filename}', open_to_write.write)
        open_to_write.close()
