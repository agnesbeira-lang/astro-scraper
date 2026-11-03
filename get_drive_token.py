"""Jalankan SEKALI di komputer Anda untuk mendapatkan refresh token Google Drive.

  pip install google-auth-oauthlib
  python get_drive_token.py credentials.json

credentials.json = OAuth client ID tipe "Desktop app" dari Google Cloud Console.
Browser akan terbuka, login dengan akun Google tempat arsip akan disimpan.
Hasilnya (3 nilai) disalin ke GitHub Secrets. Jangan commit credentials.json.
"""
import sys

from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = ["https://www.googleapis.com/auth/drive.file"]

if len(sys.argv) != 2:
    sys.exit("Pemakaian: python get_drive_token.py credentials.json")

flow = InstalledAppFlow.from_client_secrets_file(sys.argv[1], SCOPES)
creds = flow.run_local_server(port=0, access_type="offline", prompt="consent")

if not creds.refresh_token:
    sys.exit("Refresh token tidak keluar. Hapus akses app di myaccount.google.com/permissions lalu ulangi.")

print("\nSalin 3 nilai ini ke GitHub Secrets:\n")
print("DRIVE_CLIENT_ID     =", creds.client_id)
print("DRIVE_CLIENT_SECRET =", creds.client_secret)
print("DRIVE_REFRESH_TOKEN =", creds.refresh_token)
