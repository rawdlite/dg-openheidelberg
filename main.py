from src.dg_openheidelberg.defs import assets
from src.nextcloud import NextcloudClient

def main():
    #assets.processed_data()
    assets.nextcloud_user_data()
    #next_client = NextcloudClient()
    #next_client.get_users()
    #sample_data_file = "src/dg_openheidelberg/defs/data/user_onboarding.csv"
    #next_client.upload_file('user_onboarding.csv',sample_data_file)

if __name__ == "__main__":
    main()
