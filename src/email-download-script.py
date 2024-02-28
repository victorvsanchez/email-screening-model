import json
from simple_salesforce import Salesforce, SalesforceLogin 
import os
import requests

credentials = json.load(open("../credentials.json"))
instance = credentials["instance"]
username = credentials["username"]
password = credentials["password"]
token = credentials["security_token"]

session_id, instance = SalesforceLogin(username=username, password=password, security_token=token)
sf = Salesforce(instance=instance, session_id=session_id)
instance_name = sf.sf_instance

all_texts = sf.query("SELECT Id, TextBody FROM EmailMessage")
all_attachments = sf.query("SELECT Id, ParentId, ContentType FROM Attachment WHERE ParentId IN (SELECT Id FROM EmailMessage)")

folder_path = "../../../copel_attachments/"

for attachment in all_attachments["records"]:
    file_name = attachment['Name']
    email_id = attachment['ParentId']
    attachment_url = attachment['Body']
    
    if not os.path.exists(os.path.join(folder_path, email_id)):
        os.mkdir(os.path.join(folder_path))

    request = sf.session.get('https://{0}{1}'.format(instance_name, attachment_url), headers=sf.headers)

    with open(os.path.join(folder_path, email_id, file_name), 'wb') as f:
        f.write(request.content)
        f.close()