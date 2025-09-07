import dagster as dg
import json
from couchdbclient import Client
from openproject import WorkPackageParser, UserParser, CUSTOMFIELD, STATUS
from nextcloud import NextcloudClient

# TODO: move config loading to a central place
# load config
# config = Config()
# wp = WorkPackageParser(config=config.get('workpackages')) 
wp = WorkPackageParser()
up = UserParser()
next_client = NextcloudClient()

def replace_umlauts(text: str) -> str:
    """replace special German umlauts (vowel mutations) from text. 
    ä -> ae, Ä -> Ae...
    ü -> ue, Ü -> Ue...
    ö -> oe, Ö -> Oe...
    ß -> ss
    """
    vowel_char_map = {
        ord('ä'): 'ae', ord('ü'): 'ue', ord('ö'): 'oe', ord('ß'): 'ss',
        ord('Ä'): 'Ae', ord('Ü'): 'Ue', ord('Ö'): 'Oe'
    }
    return text.translate(vowel_char_map)


        
# CREATE MEMBER TASKS PIPELINE
@dg.asset(name='create_openproject_member_tasks',
          group_name="initialisation",
          description="Write initial user onboarding task from couchdb")
def create_openproject_member_tasks():
    """couch-->op
    Write initial user onboarding task to OpenProject"""
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

@dg.asset(group_name="account",
          description="op->>opu op->>next\n Create accounts")
def create_user_accounts():
    # Load OpenProject tasks with status 'scheduled'
    client = Client()
    tasks = wp.get_workpackages(status_id=STATUS['Scheduled'], project_id=18)
    if not tasks:
        return "No tasks found with status 'scheduled' in OpenProject"
    for task in tasks:
        # Get couchdb entry
        docs = client.get_doc_by_member_id(member_id=task['id'])
        if not docs:
            wp.add_comment(member_id=task['id'], comment="No CouchDB document found for this member\n Something went wrong")
            wp.update_status(task=task, status='In specification')
            continue
        elif len(docs) == 1:
            doc = docs[0]
        else:
            wp.add_comment(member_id=task['id'], comment="Multiple CouchDB documents found for this member\n Please fix this first")
            wp.update_status(task=task, status='In specification')
            continue
        task[CUSTOMFIELD['username']] = replace_umlauts(task[(CUSTOMFIELD['username'])])
        # Create openproject user accounts from task data
        if task.get(CUSTOMFIELD['openproject']):
            if doc.get('openproject'):
                # User already exists in OpenProject
                print(f"User {task[CUSTOMFIELD['firstname']]} {task[CUSTOMFIELD['lastname']]} already exists in OpenProject")
            else:
                # Create user in OpenProject
                user = up.create_new_user(task=task)
                if user:
                    op_user_info = up.user_info(user)
                    wp.add_comment(member_id=task['id'],comment=json.dumps(op_user_info))
                    doc['openproject'] = op_user_info
                    # update task status to 'in progress'
                    wp.update_status(task, 'In progress')  # Assuming status ID 7 is 'in progress'
                else:
                    wp.add_comment(member_id=task['id'],comment="Failed to create user in OpenProject")
                    wp.update_status(task, 'In specification')
                    print(f"Failed to create user in OpenProject")
        # Create nextcloud account
        if task.get(CUSTOMFIELD['nextcloud']):
            # Create user in Nextcloud
            nextcloud_user_data = {
                'username': task.get(CUSTOMFIELD['username'], ''),
                'firstname': task.get(CUSTOMFIELD['firstname'], ''),
                'lastname': task.get(CUSTOMFIELD['lastname'], ''),
                'email': task.get(CUSTOMFIELD['email'], '')
            }
            nextcloud_user = next_client.create_user(nextcloud_user_data)
            if nextcloud_user:
                nx_user_info = next_client.user_info(nextcloud_user)
                doc['nextcloud'] = nx_user_info
                wp.add_comment(member_id=task['id'], comment=json.dumps(nx_user_info))
            else:
                wp.add_comment(member_id=task['id'], comment=f"Failed to create user {nextcloud_user_data['username']} in Nextcloud")
                print(f"Failed to create user {nextcloud_user_data['username']} in Nextcloud")
        client.db.save(doc)
    return "Create user accounts task finished"
        
         
# CONSOLIDATION PIPELINE

@dg.asset(name="update_couchdb",
          group_name="consolidation",
          description="op->>couch\nUpdate CouchDB with OpenProject user task data"
          )
def update_couchdb():
    """
    get all couch docs
    op->>couch
    Update CouchDB with OpenProject user task data
    """
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
                doc['firstname'] = member[CUSTOMFIELD['firstname']]
                doc['lastname'] = member[CUSTOMFIELD['lastname']]
                doc['email'] = member[CUSTOMFIELD['email']]
                doc['username'] = member[CUSTOMFIELD['username']]
                doc['git'] = member[CUSTOMFIELD['git']]
                doc['public_key'] = member[CUSTOMFIELD['public key']]
                doc['telephone'] = member[CUSTOMFIELD['telephone']]
                doc['training'] = member['_links'][CUSTOMFIELD['training']]
                doc['altstadt'] = member[CUSTOMFIELD['altstadt']]
                doc['neuenheim'] = member[CUSTOMFIELD['neuenheim']]
                # Save the updated document back to CouchDB
                client.db.save(doc)
        else:
            # no member_id means initialisation was not run yet
            continue
    return "CouchDB updated successfully with OpenProject user task data"

@dg.asset(name="validate_user_openproject", 
          group_name="consolidation",
          deps=["update_couchdb"],
          description="opu->>couch\nValidate user data from OpenProject")
def user_openproject_data():
    """opu->>couch Load user data from OpenProject"""
    # Fetch user data from OpenProject
    up = UserParser()
    client = Client()
    res = up.get_users()
    for user in res['users']:
        # Create or update user in CouchDB
        openproject_data = up.user_info(user)
        # get couchdb document
        docs = client.get_doc_by_openproject_id(openproject_id=user['id']) or \
            client.get_doc_by_email(email=user['email'])
        if docs and len(docs) == 1:
            doc = docs[0]
            # Update existing document
            doc['openproject'] = openproject_data
        else:
            # TODO: handle this case
            # We have no document yet
            # we leave this for now until the delete workflow is specified
            continue
        # Save to CouchDB
        client.db.save(doc)
    return res

@dg.asset(name="validate_user_nextcloud",
          group_name="consolidation",
          deps=["update_couchdb"],
          description="next->>couch\nValidate user data from Nextcloud")
def user_nextcloud_data():
    """
    Load user data from Nextcloud
    all next users
    next->>couch
    """
    client = Client()
    # Fetch user data from Nextcloud
    users = next_client.get_users()
    for user in users:
        # Create or update user in CouchDB
        userinfo = next_client.get_user(user_id=user['id'])
        if not userinfo:
            continue
        nextcloud_data = next_client.user_info(userinfo)
        # get couchdb document
        
        res = client.get_doc_by_nextcloud_id(nextcloud_id=nextcloud_data['nextcloud_id']) or \
            client.get_doc_by_email(email=nextcloud_data['nextcloud_email'])
        if res and len(res) == 1:
            # Update existing document
            doc = res[0]    
            doc['nextcloud'] = nextcloud_data
        elif res and len(res) > 1:
            # TODO: handle and log this case
            continue
        else:
            # TODO: handle and log this case
            # We have a nextcloud user but no document yet
            # we leave this for now until the delete workflow is specified
            continue
        
        # Save to CouchDB
        client.db.save(doc)
    return res



@dg.asset(name="update_openproject_member_tasks",
          group_name="consolidation",
          description="couch->>op\nUpdate OpenProject member tasks from couchdb entries")
def update_openproject_member_tasks():
    """
    get all op entries
    couch->>op
    Update OpenProject member tasks from CouchDB entries
    """
    client = Client()
    # Fetch all member tasks in Status In progress
    tasks = wp.get_workpackages(status_id=STATUS['In progress'], project_id=18)
    for member in tasks:
        docs = client.get_doc_by_member_id(member_id=member['id'])   
        if not docs:
            wp.add_comment(member_id=member['id'], comment="No CouchDB document found for this member")
            wp.update_status(task=member, status='In specification')            
            continue
        elif len(docs) == 1:
            wp.update_member_task(doc=docs[0], member=member)
            wp.update_status(task=member, status='In progress')
        else:
            wp.add_comment(member_id=member['id'], comment="Multiple CouchDB documents found for this member")
            wp.update_status(task=member, status='In specification')
    # Return a success message
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
