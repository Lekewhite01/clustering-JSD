
"""
Modularized data processing system with clean separation of concerns.
"""

# Database
MONGO_URI = "mongodb+srv://sabi-prod-user:PDxhVSntkhg8kLx5@pastelcluster.e5bi3.mongodb.net/sabi-api-proddb?retryWrites=true&w=majority"
DB_NAME = "sabi-api-proddb"
WALLET_COLLECTION = "userwallettransactions"
USER_COLLECTION = "users"
# AWS
BUCKET_NAME = "historical-dataset"

# # Feature Constants
# FEATURE_COLUMNS = [
#     'amount', 'preBalance', 'aggregate_30_amount_sum',
#     'aggregate_30_preBalance_sum', 'aggregate_30_amount_mean',
#     'aggregate_30_preBalance_mean', 'aggregate_30_count',
#     'aggregate_60_amount_sum', 'aggregate_60_preBalance_sum',
#     'aggregate_60_amount_mean', 'aggregate_60_preBalance_mean',
#     'aggregate_60_count', 'recent_transactions', 'firstTransfer',
#     'channel_atm', 'channel_pos', 'channel_transfer'
# ]