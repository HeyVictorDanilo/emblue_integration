from database import main_db
import pysftp
from datetime import date


class EmblueConnection(pysftp.Connection):
    def __init__(self, *args, **kwargs):
        self._sftp_live = False
        self._transport = None
        super().__init__(*args, **kwargs)


class Emblue:
    def __init__(self):
        self.db_instance = main_db.DBInstance(public_key="kKS0DfTKpE8TqUZs")

    def get_emblue_accounts(self):
        accounts = self.db_instance.handler(query="SELECT * FROM em_blue;")
        return accounts

    def download_file(self):
        for account in self.get_emblue_accounts():
            with EmblueConnection(account[2], username=account[4], password=account[3]) as sftp:
                print(sftp.pwd())
                with sftp.cd('upload/Report'):
                    today = date.today().strftime("%Y%m%d")
                    sftp.get(f"ACTIVIDADDETALLEDIARIOFTP{today}.zip")


if __name__ == '__main__':
    emblue = Emblue()
    emblue.download_file()
