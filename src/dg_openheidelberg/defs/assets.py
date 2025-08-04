import pandas as pd
import dagster as dg
from couchdbclient import Client
from openproject import WorkPackageParser, UserParser
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
        
#ToDo write to couchdb
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


@dg.asset(name="user_initialisation", description="Write initial user onboarding data to openproject")
def user_initialisation():
    """Write initial user onboarding data to OpenProject"""
    #initialize couchdb client
    client = Client()
    # Fetch documents without 'openproject' key
    docs = client.get_docs_without_openproject_key()
    for doc in docs:
        payload = {
            'customField5': doc.get('firstname', ''),
            'customField6': doc.get('lastname', ''),
            'customField7': doc.get('email', ''),
            'subject': doc.get('_id', 'New User'),
            'status_id': 2, # in specification
            'projectId': '18'  # Assuming project ID is 18
        }
        # Create user in OpenProject
        res = wp.create_member(payload)
        if res:
            doc['openproject'] = {
                'status': res['status'],
                'admin': res['admin'],
                'createdAt': res['createdAt']
            }
            client.update_doc(doc)
        return res
    
@dg.asset(name="validate_user_openproject", description="Validate user data from OpenProject")
def user_openproject_data():
    """Load user data from OpenProject"""
    # Fetch user data from OpenProject
    up = UserParser()
    client = Client()
    res = up.get_users()
    for user in res['users']:
        # Create or update user in CouchDB
        
        openproject_data = {
            'openproject_id': user.get('id', ''),
            'openproject_login': user.get('login', ''),
            'openproject_firstname': user.get('firstName', ''),
            'openproject_lastname': user.get('lastName', ''),
            'openproject_email': user.get('email', ''),
            'openproject_status': user.get('status', ''),
            'openproject_admin': user.get('admin', ''),
            'openproject_created_at': user.get('createdAt', ''),
            'openproject_updated_at': user.get('updatedAt', ''),
            'openproject_language': user.get('language', ''),
            'openproject_matrix': user.get('customField3', '')
            }
        doc_id = f"{user.get('firstName', '')}.{user.get('lastName', '')}".replace(" ", "_")
        # get couchdb document
        doc = client.db.get(doc_id)

        if doc:
            # Update existing document
            doc['openproject'] = openproject_data
        else:
            # Create new document
            doc = {
                '_id': doc_id,
                'firstname': user.get('firstName', ''),
                'lastname': user.get('lastName', ''),
                'email': user.get('email', ''),
                'username': f"{user.get('firstName', '')[0]}{user.get('lastName', '')}".lower().replace(" ", ""),
                'openproject': openproject_data
            }       
        
        #print(doc)
        # Save to CouchDB
        client.db.save(doc)
    return res

@dg.asset(name="validate_user_nextcloud", description="Validate user data from Nextcloud")
def user_nextcloud_data():
    """Load user data from Nextcloud"""
    # Fetch user data from Nextcloud
    res = next_client.get_users()
    for user in res:
        # Create or update user in CouchDB
        nextcloud_data = {
            'nextcloud_id': user.get('id', ''),
            'nextcloud_displayname': user.get('displayname', ''),
            'nextcloud_email': user.get('email', ''),
            'nextcloud_status': user.get('enabled', ''),
            'nextcloud_last_login': user.get('last_login', '')
        }
        doc_id = f"{user.get('displayname', '')}".replace(" ", ".")
        # get couchdb document
        client = Client()
        doc = client.db.get(doc_id)
        # if not doc:
        #     mango_filter = Client.mango_filter_by_email(nextcloud_data['nextcloud_email'])
        #     doc = client.db.find({"selector": mango_filter})
        #     if isinstance(doc, list) and doc:
        #         doc = doc[0]
        if doc:
            # Update existing document
            doc['nextcloud'] = nextcloud_data
        else:
            # Create new document
            firstname =  user.get('displayname', '').split(' ')[0]
            lastname =  user.get('displayname', '').split(' ')[-1]
            doc = {
                '_id': doc_id,
                'firstname': firstname,
                'lastname': lastname,
                'email': nextcloud_data['nextcloud_email'],
                'username': f"{firstname[0]}{lastname}".lower().replace(" ", ""),
                'nextcloud': nextcloud_data
            }
        
        # Save to CouchDB
        client.db.save(doc)
    return res



@dg.asset(name="create_openproject_member_tasks",
          description="Create OpenProject member tasks from couchdb entries",
            deps=["validate_user_openproject","validate_user_nextcloud"])
def create_openproject_member_tasks():
    """Create OpenProject member tasks from CouchDB entries"""
    client = Client()
    wp = WorkPackageParser()
    for row in client.db.view('_all_docs', include_docs=True):
        if row['id'].startswith('_design/'):
            continue
        doc = row.get('doc', {})
        if wp.check_member_exists(
            subject=doc.get('_id', ''),
            firstname=doc.get('firstname', ''),
            lastname=doc.get('lastname', ''),
            username=doc.get('username', ''),
            email=doc.get('email', '')):
            # User already exists in OpenProject, skip for now
            # TODO: Update existing users
            continue
        # Create a new member task in OpenProject
        username = f"{doc.get('firstname', '')[0]}{doc.get('lastname', '')}".lower().replace(" ", "")
        payload = {
            'customField5': doc.get('firstname', ''),
            'customField6': doc.get('lastname', ''),
            'customField7': doc.get('email', ''),
            'customField20': username,
            'subject': doc.get('_id'),
            'status_id': 7, # in progress
            'projectId': '18',  # Assuming project ID is 18
            'customfield10': doc.get('nextcloud', "") != "",
            'customfield8': doc.get('openproject', "") != ""
        }
        wp.create_member(payload)
    
    return "OpenProject member tasks created successfully"



dg.asset(name="create user accounts",
         description="Create user accounts from OpenProject and Nextcloud data")
def create_user_accounts():
    """Create user accounts from OpenProject and Nextcloud data"""
    # Load OpenProject tasks with status 'scheduled'
    wp = WorkPackageParser()
    up = UserParser()
    tasks = wp.get_workpackages(status_id=6, project_id=18)
    if not tasks:
        return "No tasks found with status 'scheduled' in OpenProject"
    for task in tasks:
        # Create user accounts from task data
        if task.get('customField8'):
            new_user_data = {
                'firstName': task.get('customField5', ''),
                'lastName': task.get('customField6', ''),
                'login': task.get('customField20', ''),
                'email': task.get('customField7', ''),
                'status': 'invited'  # Assuming status is invited for new users
            }
            # Create user in OpenProject
            
            user = up.create_user(new_user_data)
            if user:
                print(f"User {new_user_data['login']} created successfully in OpenProject")
                # update task status to 'in progress'
                wp.update_status(task, 7)  # Assuming status ID 7 is 'in progress'
            else:
                print(f"Failed to create user {new_user_data['login']} in OpenProject")
        if task.get('customField10'):
            # Create user in Nextcloud
            nextcloud_user_data = {
                'username': task.get('customField20', ''),
                'firstname': task.get('customField5', ''),
                'lastname': task.get('customField6', ''),
                'email': task.get('customField7', '')
            }
            nextcloud_user = next_client.create_user(firstname=task.get('customField5', ''),
                                                     lastname=task.get('customField6', ''),
                                                     email=task.get('customField7', ''),
                                                     username=task.get('customField20', '')
                                                     )
            if nextcloud_user:
                print(f"User {nextcloud_user_data['username']} created successfully in Nextcloud")
                # update task status to 'in progress'
                wp.update_status(task, 7)
            else:
                print(f"Failed to create user {nextcloud_user_data['username']} in Nextcloud")
    return "User accounts created successfully from OpenProject and Nextcloud data"
         