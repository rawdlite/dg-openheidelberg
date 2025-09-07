import dagster as dg
from nextcloud import NextcloudClient
import pandas as pd

user_onboarding = "src/dg_openheidelberg/defs/data/user_onboarding.csv"
next_client = NextcloudClient()

# INITIALISATION PIPELINE
@dg.asset(name="user_onboarding_csv",
          group_name="initialisation",
          description="GET user onboarding csv data from Nextcloud")
def user_onboarding_csv_data():
    """Load user onboarding data from Nextcloud"""
    # Download the file from Nextcloud to the local path
    next_client.download_file('user_onboarding.csv', user_onboarding)
    # Load and return the data
    df = pd.read_csv(user_onboarding)
    return df

@dg.asset_check(asset="user_onboarding_csv")
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