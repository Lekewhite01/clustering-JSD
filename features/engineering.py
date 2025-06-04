import sys
import os
# Get the absolute path to the project root
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
import numpy as np

def process_data(wallet):
    wallet["preBalance"] = pd.to_numeric(wallet["preBalance"], errors='coerce')
    wallet["amount"] = pd.to_numeric(wallet["amount"], errors='coerce')
    # wallet["postBalance"] = pd.to_numeric(wallet["postBalance"], errors='coerce')
    
    aggregate_30_sum = wallet.sort_values(["user", "transactionDate"]).groupby("user").rolling('30d', on='transactionDate')["amount", "preBalance"].sum().reset_index().rename(columns={"amount":"aggregate_30_amount_sum",
                                                                                                                                           "preBalance":"aggregate_30_preBalance_sum"
                                                                                                                                          })
    
    aggregate_30_mean = wallet.sort_values(["user", "transactionDate"]).groupby("user").rolling('30d', on='transactionDate')["amount", "preBalance"].mean().reset_index().rename(columns={"amount":"aggregate_30_amount_mean",
                                                                                                                                           "preBalance":"aggregate_30_preBalance_mean"
                                                                                                                                          })
    aggregate_30_count = wallet.sort_values(["user", "transactionDate"]).groupby("user").rolling('30d', on='transactionDate')["amount"].count().reset_index(name="aggregate_30_count")
    
    aggregate_60_sum = wallet.sort_values(["user", "transactionDate"]).groupby("user").rolling('60d', on='transactionDate')["amount", "preBalance"].sum().reset_index().rename(columns={"amount":"aggregate_60_amount_sum",
                                                                                                                                           "preBalance":"aggregate_60_preBalance_sum"
                                                                                                                                          })
    
    aggregate_60_mean = wallet.sort_values(["user", "transactionDate"]).groupby("user").rolling('60d', on='transactionDate')["amount", "preBalance"].mean().reset_index().rename(columns={"amount":"aggregate_60_amount_mean",
                                                                                                                                           "preBalance":"aggregate_60_preBalance_mean"
                                                                                                                                          })
    aggregate_60_count = wallet.sort_values(["user", "transactionDate"]).groupby("user").rolling('60d', on='transactionDate')["amount"].count().reset_index(name="aggregate_60_count")
    
    
    aggregate_30_sum = aggregate_30_sum.groupby(["user", "transactionDate"])[["aggregate_30_amount_sum", "aggregate_30_preBalance_sum"]].max().reset_index()
    aggregate_30_mean = aggregate_30_mean.groupby(["user", "transactionDate"])[["aggregate_30_amount_mean", "aggregate_30_preBalance_mean"]].max().reset_index()
    aggregate_30_count = aggregate_30_count.groupby(["user", "transactionDate"])["aggregate_30_count"].max().reset_index()
    
    aggregate_60_sum = aggregate_60_sum.groupby(["user", "transactionDate"])[["aggregate_60_amount_sum", "aggregate_60_preBalance_sum"]].max().reset_index()
    aggregate_60_mean = aggregate_60_mean.groupby(["user", "transactionDate"])[["aggregate_60_amount_mean", "aggregate_60_preBalance_mean"]].max().reset_index()
    aggregate_60_count = aggregate_60_count.groupby(["user", "transactionDate"])["aggregate_60_count"].max().reset_index()
    
    
    wallet = wallet.merge(aggregate_30_sum, on=["user", "transactionDate"])
    wallet = wallet.merge(aggregate_30_mean, on=["user", "transactionDate"])
    wallet = wallet.merge(aggregate_30_count, on=["user", "transactionDate"])
    wallet = wallet.merge(aggregate_60_sum, on=["user", "transactionDate"])
    wallet = wallet.merge(aggregate_60_mean, on=["user", "transactionDate"])
    wallet = wallet.merge(aggregate_60_count, on=["user", "transactionDate"])
    
    wallet["recent_transactions"] = wallet.sort_values(["user", "transactionDate"]).groupby("user")["transactionDate"].diff(periods=1)
    wallet["recent_transactions"].fillna(method="ffill", inplace=True)
    wallet["recent_transactions"] = wallet["recent_transactions"].dt.total_seconds()
    
    wallet["firstTransfer"] = 0
    wallet.loc[wallet["preBalance"]==0.0, "firstTransfer"] = 1
    
    wallet["transactionLocation"].fillna(method="ffill", inplace=True)
    wallet["transactionLocation"].fillna("", inplace=True)
    wallet[["transactionLocationLat", "transactionLocationLong"]] = pd.DataFrame(wallet["transactionLocation"].str.split(",").tolist(), index=wallet.index)
    
    wallet["transactionLocationLat"].replace("", np.nan, inplace=True)
    wallet["transactionLocationLong"].replace("", np.nan, inplace=True)
    wallet["transactionLocationLat"].fillna(method="ffill", inplace=True)
    wallet["transactionLocationLong"].fillna(method="ffill", inplace=True)
    
    wallet["transactionLocationLat"] = wallet["transactionLocationLat"].astype(float)
    wallet["transactionLocationLong"] = wallet["transactionLocationLong"].astype(float)
    
    wallet["transactionLocationLat"].fillna(0, inplace=True)
    wallet["transactionLocationLong"].fillna(0, inplace=True)
    
    cat_dummies = pd.get_dummies(wallet["channel"], prefix="channel")
    wallet = pd.concat([wallet, cat_dummies], axis=1)
    
    wallet.drop(['channel','transactionLocation'], axis=1, inplace=True)
    
    return wallet