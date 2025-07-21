import pandas as pd
import dagster as dg
from openproject import WorkPackageParser
from nextcloud import NextcloudClient

user_onboarding = "src/dg_openheidelberg/defs/data/user_onboarding.csv"
openproject_user_data_file = "src/dg_openheidelberg/defs/data/openproject_user.csv"
nextcloud_user_data_file = "src/dg_openheidelberg/defs/data/nextcloud_user.csv"
merged_user_data_file = "src/dg_openheidelberg/defs/data/accounts.csv"
wp = WorkPackageParser()
next_client = NextcloudClient()


def get_nextcloud_status(row):
    user = next_client.check_user(username = row['username'],
                         firstname = row['firstname'],
                         lastname = row['lastname'],
                         email = row['email'])
    if user:
        return pd.Series({
            'nextcloud_status': user['enabled'],
            'nextcloud_email': user['email'],
            'nextcloud_last': user['last_login']
        })
    else:
        return pd.Series({
            'nextcloud_status': 'not found',
            'nextcloud_email': '',
            'nextcloud_last': ''
        })

def get_openproject_status(row):
    user = wp.check_user(username = row['username'],
                         firstname = row['firstname'],
                         lastname = row['lastname'],
                         email = row['email'])
    if user:
        return pd.Series({
            'openproject_status': user['status'],
            'openproject_admin': user['admin'],
            'openproject_created_at': user['createdAt']})
    else:
        return pd.Series({
            'openproject_status': 'not found',
            'openproject_admin': '',
            'openproject_created_at': ''})

@dg.asset(name="user_onboarding", description="GET user onboarding data from Nextcloud")
def user_onboarding_data():
    """Load user onboarding data from Nextcloud"""
    # Download the file from Nextcloud to the local path
    next_client.download_file('user_onboarding.csv', user_onboarding)
    # Load and return the data
    df = pd.read_csv(user_onboarding)
    return df

@dg.asset_check(asset="user_onboarding")
def check_user_onboarding_has_email_data():
    """Check that user_onboarding contains email column with valid email addresses."""
    try:
        df = pd.read_csv(user_onboarding)
        # Check if email column exists
        if 'email' not in df.columns:
            return dg.AssetCheckResult(
                passed=False,
                description="Email column is missing from the onboarding data file"
            )
        
        # Check if email column has data (non-empty values)
        email_count = df['email'].notna().sum()
        total_rows = len(df)
        
        if email_count == 0:
            return dg.AssetCheckResult(
                passed=False,
                description="Email column exists but contains no data"
            )
        
        # Check for valid email format (basic validation)
        valid_emails = df['email'].str.contains('@', na=False).sum()
        
        return dg.AssetCheckResult(
            passed=True,
            description=f"Email data validated: {valid_emails} valid emails out of {total_rows} total rows, {valid_emails} with valid format",
            metadata={
                "total_rows": dg.MetadataValue.int(int(total_rows)),
                "emails_with_data": dg.MetadataValue.int(int(email_count)),
                "valid_email_format": dg.MetadataValue.int(int(valid_emails))
            }
        )
        
    except Exception as e:
        return dg.AssetCheckResult(
            passed=False,
            description=f"Failed to read or validate sample data file: {str(e)}"
        )

@dg.asset(name="openproject_user_data",
          description="add user data from Openproject",
          deps=["user_onboarding"])
def openproject_user_data():
    ## Read data from the CSV
    df = pd.read_csv(user_onboarding)
    ## Process data
    df[['openproject_status','openproject_admin','openproject_created_at']] = df.apply(get_openproject_status, axis=1)
    ## Save processed data
    df.to_csv(openproject_user_data_file, index=False)
    return "Data loaded successfully"

@dg.asset(name="nextcloud_user_data",
          description="add user data from Nextcloud",
          deps=["user_onboarding"])
def nextcloud_user_data():
    ## Read data from the CSV
    df = pd.read_csv(user_onboarding)
    ## Process data
    df[['nextcloud_status', 'nextcloud_email', 'nextcloud_last']] = df.apply(get_nextcloud_status, axis=1)
    df.to_csv(nextcloud_user_data_file, index=False)
    return "Data loaded successfully"

@dg.asset(name="upload_nextcloud_user_data",
          description="Upload Processed data to Nextcloud",
          deps=["nextcloud_user_data"])
def upload_nextcloud_user_data():
    next_client.upload_file('nextcloud_user_data.csv', nextcloud_user_data_file)


@dg.asset(name="upload_openproject_user_data",
          description="Upload Processed data to Nextcloud",
          deps=["openproject_user_data"])
def upload_openproject_user_data():
    next_client.upload_file('openproject_user_data.csv', openproject_user_data_file)

@dg.asset(name="merge_user_data",
          description="Join Dataframes",
          deps=["openproject_user_data","nextcloud_user_data"])
def merge_user_data():
    op = pd.read_csv(openproject_user_data_file)
    nx = pd.read_csv(nextcloud_user_data_file)
    res = pd.merge(op,nx,how='outer',on=['firstname','lastname','email','username'])
    res['action request'] = ""
    res.to_csv(merged_user_data_file,index=False)

@dg.asset(name="upload_merged_data",
          description="upload the merged user data",
          deps=["merge_user_data"])
def upload_merged_data():
    next_client.upload_file('/Admin/accounts.csv', merged_user_data_file)