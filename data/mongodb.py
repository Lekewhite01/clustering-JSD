import sys
import os
# Get the absolute path to the project root
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pymongo import MongoClient
from bson.objectid import ObjectId
import pandas as pd
import json
# Use absolute import instead of relative import
from config import MONGO_URI, DB_NAME, WALLET_COLLECTION, USER_COLLECTION

def get_wallet_pipeline():
    return [
        {'$match': {}},
        {'$replaceRoot': {'newRoot': {'$mergeObjects': ["$response.data", "$$ROOT"]}}},
        {'$project': {
            '_id': 1, 'narration': 1, 'senderAccountNumber': 1, 
            'receiverAccountNumber': 1, 'amount': 1, 'transactionDate': 1, 'status': 1, 
            'type': 1, 'preBalance': 1, 'user': 1, "fraudulent": 1, 
            "channel": 1, "transactionLocation": 1
        }}
    ]

def get_user_pipeline():
    return [
        {'$match': {}},
        {'$replaceRoot': {'newRoot': {'$mergeObjects': ["$$ROOT"]}}},
        {'$project': { 
            # Required fields
            "_id": 1,
            "__v": 1,
            "accountPin": 1,
            "accountType": 1,
            "bankAccounts": 1,
            "businessEmployees": 1,
            "businessName": 1,
            "businessReferredCount": 1,
            "businessReferredFirstSalesCount": 1,
            "canAccrueRefPoints": 1,
            "createdAt": 1,
            "fcmToken": 1,
            "firstName": 1,
            "isActive": 1,
            "lastName": 1,
            "phone": 1,
            "phoneVerified": 1,
            "referralCode": 1,
            "referralPointsBalance": 1,
            "resetCode": 1,
            "resetCodeExpiry": 1,
            "role": 1,
            "secondaryBusinesses": 1,
            "tempData": 1,
            "updatedAt": 1,
            
            # Optional fields
            "address": 1,
            "ajoGroups": 1,
            "ajoUserCategory": 1,
            "androidSignedUserId": 1,
            "apps": 1,
            "businessAddress": 1,
            "businessCategory": 1,
            "businessCity": 1,
            "businessFreshChatRestoreId": 1,
            "businessLogoUrl": 1,
            "businessState": 1,
            "businessTagLine": 1,
            "businessTypeId": 1,
            "bvn": 1,
            "bvnData": 1,
            "bvnVerified": 1,
            "city": 1,
            "country": 1,
            "deviceHash": 1,
            "dob": 1,
            "email": 1,
            "emailResetCode": 1,
            "emailResetCodeExpiry": 1,
            "emailVerified": 1,
            "experience": 1,
            "freshChatRestoreId": 1,
            "gender": 1,
            "is2faEnabled": 1,
            "isBanned": 1,
            "isDeleted": 1,
            "isSavingsActive": 1,
            "lastActivity": 1,
            "lastLogin": 1,
            "loanDefaults": 1,
            "loans": 1,
            "logoType": 1,
            "note": 1,
            "occupation": 1,
            "os": 1,
            "password": 1,
            "permission": 1,
            "referenceId": 1,
            "referredBy": 1,
            "signatureBankStatement": 1,
            "signatureInterests": 1,
            "state": 1,
            "tempFlag": 1,
            "tier": 1,
            "token": 1,
            "totalSavings": 1
        }}
    ]

# def get_user_pipeline():
#     return [
#         {'$match': {}},
#         {'$replaceRoot': {'newRoot': {'$mergeObjects': ["$$ROOT"]}}},
#         {'$project': { 
#             "_id": 1, "phoneVerified": 1, "createdAt": 1, "isBanned": 1, "dob": 1,
#             "bvnVerified": 1, "state": 1, "city": 1, "country": 1, "os": 1
#         }}
#     ]

def read_mongo(db, collection, pipeline):
    """Read from MongoDB and return as DataFrame"""
    cursor = db[collection].aggregate(pipeline)
    return pd.json_normalize(list(cursor))

def get_mongo_data():
    """Get wallet and user data from MongoDB"""
    client = MongoClient(host=MONGO_URI)
    db = client[DB_NAME]
    
    wallet_pipeline = get_wallet_pipeline()
    user_pipeline = get_user_pipeline()
    
    wallet_df = read_mongo(db, WALLET_COLLECTION, wallet_pipeline)
    user_df = read_mongo(db, USER_COLLECTION, user_pipeline)
    
    return wallet_df, user_df