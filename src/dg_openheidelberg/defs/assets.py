import pandas as pd
import dagster as dg
import json
from couchdbclient import Client
from config import Config
from openproject import WorkPackageParser, UserParser, CUSTOMFIELD, STATUS
from nextcloud import NextcloudClient

user_onboarding = "src/dg_openheidelberg/defs/data/user_onboarding.csv"
openproject_user_data_file = "src/dg_openheidelberg/defs/data/openproject_user.csv"
nextcloud_user_data_file = "src/dg_openheidelberg/defs/data/nextcloud_user.csv"
merged_user_data_file = "src/dg_openheidelberg/defs/data/accounts.csv"

wp = WorkPackageParser()
up = UserParser()
next_client = NextcloudClient()

# INITIALISATION PIPELINE
@dg.asset(name="user_onboarding_csv", description="GET user onboarding csv data from Nextcloud")
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
        
# CREATE MEMBER TASKS PIPELINE
@dg.asset(name="user_initialisation", 
          description="Write initial user onboarding task from couchdb",
          deps=["user_onboarding_csv"])
def user_initialisation():
    """Write initial user onboarding task to OpenProject"""
    #initialize couchdb client
    client = Client()
    # Fetch documents without 'openproject' key
    docs = client.get_docs_without_member_id()
    for doc in docs:
        doc = fix_doc_id(doc)
        if not doc.get('username'):
            username = f"{doc.get('firstname', '')[0]}{doc.get('lastname', '')}".lower().replace(" ", "")
            doc['username'] = username
            
        member = wp.initialize_member_from_doc(doc=doc)
        if member:
            doc['member_id'] = member['id']
            # Save the updated document back to CouchDB
            client.db.save(doc)
    # Return a success message
    return {"status": "success", "message": "User initialization completed successfully."}
    
# CREATE ACCOUNTS PIPELINES

dg.asset(name="create openproject user accounts",
         description="Create openproject user accounts from OpenProject Task data")
def create_openproject_user_accounts():
    """Create openprojectuser accounts from OpenProject task data"""
    # Load OpenProject tasks with status 'scheduled'
    client = Client()
    tasks = wp.get_workpackages(status_id=6, project_id=18)
    if not tasks:
        return "No tasks found with status 'scheduled' in OpenProject"
    for task in tasks:
        doc = client.get_doc_by_member_id(member_id=task['id'])
        if not doc:
            #TODO: Handle case where doc is None
            # We have a task but no document yet
            # we leave this for now until the delete workflow is specified
            print(f"No CouchDB document found for member_id {task['id']}")
            continue
        # Create user accounts from task data
        if task.get('customField8'):
            if doc.get('openproject'):
                # User already exists in OpenProject
                print(f"User {task['customField5']} {task['customField6']} already exists in OpenProject")
            else:
                # Create user in OpenProject
                user = up.create_new_user(task=task)
                if user:
                    user_info = up.user_info(user)
                    wp.add_comment(member_id=task['id'],comment=json.dumps(user_info))
                    doc['openproject'] = user_info
                    client.db.save(doc)
                    # update task status to 'in progress'
                    wp.update_status(task, 'In progress')  # Assuming status ID 7 is 'in progress'
                else:
                    wp.add_comment(member_id=task['id'],comment="Failed to create user in OpenProject")
                    print(f"Failed to create user in OpenProject")
    return "OpenProject user accounts created successfully from task data"  

dg.asset(name="create nextcloud user accounts",
         description="Create nextcloud user accounts from OpenProject task data")
def create_nextcloud_user_accounts():
    """Create nextcloud user accounts from OpenProject task data"""
    # Load OpenProject tasks with status 'scheduled'
    wp = WorkPackageParser()
    up = UserParser()
    client = Client()
    tasks = wp.get_workpackages(status_id=6, project_id=18)
    if not tasks:
        return "No tasks found with status 'scheduled' in OpenProject"
    for task in tasks:
        doc = client.get_doc_by_member_id(member_id=task['id'])
        # Create user accounts from task data
        if task.get('customField10'):
            # Create user in Nextcloud
            nextcloud_user_data = {
                'username': task.get('customField20', ''),
                'firstname': task.get('customField5', ''),
                'lastname': task.get('customField6', ''),
                'email': task.get('customField7', '')
            }
            nextcloud_user = next_client.create_user(nextcloud_user_data)
            if nextcloud_user:
                user_info = next_client.user_info(nextcloud_user)
                print(f"User {nextcloud_user_data['username']} created successfully in Nextcloud")
                wp.add_comment(member_id=task['id'], comment=json.dumps(user_info))
                if doc:
                    doc['nextcloud'] = user_info
                    client.db.save(doc)
                else:
                    #TODO: Handle case where doc is None
                    print(f"No CouchDB document found for nextcloud_id {user_info['nextcloud_id']}")
                # update task status to 'in progress'
                wp.update_status(task, 'In progress')
            else:
                wp.add_comment(member_id=task['id'], comment=f"Failed to create user {nextcloud_user_data['username']} in Nextcloud")
                print(f"Failed to create user {nextcloud_user_data['username']} in Nextcloud")
    return "User accounts created successfully from OpenProject and Nextcloud data"
         
# CONSOLIDATION PIPELINE

@dg.asstet(name="update couchdb",
           description="Update CouchDB with OpenProject user task data",
           )
def update_couchdb():
    """Update CouchDB with OpenProject user task data"""
    wp = WorkPackageParser()
    client = Client()
    #get all documents from CouchDB
    for doc in client.get_all_docs():
        if doc['member_id']:
            member = wp.get_member(doc['member_id'])
            if not member:
                # TODO: Handle missing member case
                # we have a member id yet no member entry in OpenProject
                # we should consider deleting accounts in this branch
                # alternativly we could create a new member entry with a delete subject
                print(f"Member with ID {doc['member_id']} not found in OpenProject")
                continue
            else:
                # Update the document with OpenProject user task data
                doc['openproject'] = {
                    'firstname': member[CUSTOMFIELD['firstname']],
                    'lastname': member[CUSTOMFIELD['lastname']],
                    'email': member[CUSTOMFIELD['email']],
                    'username': member[CUSTOMFIELD['username']],
                    'git': member[CUSTOMFIELD['git']],
                    'public_key': member[CUSTOMFIELD['public_key']],
                    'telephone': member[CUSTOMFIELD['telephone']],
                    'training': member[CUSTOMFIELD['training']],
                    'altstadt': member[CUSTOMFIELD['altstadt']],
                    'neuenheim': member[CUSTOMFIELD['neuenheim']]
                }
                # Save the updated document back to CouchDB
                client.db.save(doc)
        else:
            # no member_id means initialisation was not run yet
            continue
    return "CouchDB updated successfully with OpenProject user task data"

@dg.asset(name="validate_user_openproject", description="Validate user data from OpenProject")
def user_openproject_data():
    """Load user data from OpenProject"""
    # Fetch user data from OpenProject
    up = UserParser()
    client = Client()
    res = up.get_users()
    for user in res['users']:
        # Create or update user in CouchDB
        openproject_data = up.user_info(user)
        # get couchdb document
        doc = client.get_doc_by_member_id(member_id=user['id'])
        if doc:
            # Update existing document
            doc['openproject'] = openproject_data
        else:
            # TODO: handle this case
            # We have no document yet
            # we leave thi for now until the delete workflow is specified
            continue
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
        nextcloud_data = next_client.user_info(user)
        # get couchdb document
        client = Client()
        doc = client.get_doc_by_nextcloud_id(nextcloud_id=nextcloud_data['nextcloud_id'])
        if doc:
            # Update existing document
            doc['nextcloud'] = nextcloud_data
        else:
            # TODO: handle and log this case
            # We have a nextcloud user but no document yet
            # we leave this for now until the delete workflow is specified
            continue
        
        # Save to CouchDB
        client.db.save(doc)
    return res



@dg.asset(name="update_openproject_member_tasks",
          description="Update OpenProject member tasks from couchdb entries",
            deps=["validate_user_openproject","validate_user_nextcloud"])
def update_openproject_member_tasks():
    """Update OpenProject member tasks from CouchDB entries"""
    client = Client()
    wp = WorkPackageParser()
    for row in client.db.view('_all_docs', include_docs=True):
        if row['id'].startswith('_design/'):
           continue
        doc = row.get('doc', {})
        # Ensure the document ID is in lowercase
        doc = fix_doc_id(doc)
        if not doc.get('username'):
            username = f"{doc.get('firstname', '')[0]}{doc.get('lastname', '')}".lower().replace(" ", "")
            doc['username'] = username
        if 'member_id' in doc:
            member = wp.update_member_task(doc=doc)
            if member:
                doc['member_id'] = member['id']
                client.db.save(doc)
        else:
        #TODO: Handle case where member_id is not present
            continue
        #    member = wp.check_member_exists(
        #        subject=doc.get('_id', ''),
        #        firstname=doc.get('firstname', ''),
        #        lastname=doc.get('lastname', ''),
        #        username=doc.get('username', ''),
        #        email=doc.get('email', ''))
        #    if member:
        #        doc['member_id'] = member['id']
        #if member:
        #    # User already exists in OpenProject
        #    # Update existing users member_task
        #    wp.fix_member_task(member_task=member,doc=doc)
        #else:
        # member = wp.initialize_member_from_doc(doc=doc)
        
    # Return a success message
    print("OpenProject member tasks created successfully")
        
    
    return "OpenProject member tasks created successfully"
  
                
def fix_doc_id(doc: dict) -> dict:
    """
    Fix the document ID to ensure it is in the correct format.
    """
    client = Client()
    doc_id = doc['_id'] 
    if doc_id != doc_id.lower():
        # Update the document ID to lowercase
        doc['_id'] = doc_id.lower()
        doc.pop('_rev')
        res = client.db.save(doc)
        if res[0] == doc['_id']:
            old_doc = client.db.get(doc_id)
            client.db.delete(old_doc)
        print(f"Updated document ID from {doc_id} to {doc['_id']}")
    return doc
