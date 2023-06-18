# Autor: Willian Gomes | Analyst Payment candidate
import pandas as pd
import os
import time

url = "https://gist.githubusercontent.com/cloudwalk-tests/76993838e65d7e0f988f40f1b1909c97/raw/9ceae962009236d3570f46e59ce9aa334e4e290f/transactional-sample.csv"

transactions = pd.read_csv(url)
df_original = pd.DataFrame(transactions)
df = df_original.copy()

# Implementing new features to aid in transaction decision-making

# Converting boolean to int and renaming columns to facilitate future manipulations
df['has_cbk'] = df['has_cbk'].astype(int)
df = df.rename(columns={"transaction_date": "date", "transaction_amount": "amount"})
df['date'] = pd.to_datetime(df['date'])

# Creating a support dataframe with normal and fraud transactions 
df_normal = df[df['has_cbk'] == 0]
df_fraud = df[df['has_cbk'] == 1]

# Calculating mean and max per user_id for normal transactions only
feature_amount = df_normal.groupby('user_id')['amount'].agg(['mean', 'max']).round(2).rename(columns={'mean': 'mean_amount', 'max': 'max_amount'})

# Merging mean column with the original DataFrame
df = pd.merge(df, feature_amount, on='user_id', how='left')
df.fillna(0, inplace=True)
df.sort_values('date', inplace=True)

# Creating a fraud_score to monitor critical users
df['fraud_score'] = df.groupby('user_id', group_keys=False)['has_cbk'].apply(lambda x: x.cumsum())

# Creating a mean_score to monitor anomalous transaction behavior
df['over_mean'] = (df['amount'] > df['mean_amount']).astype(int) 
df['mean_score'] = df.groupby('user_id', group_keys=False)['over_mean'].apply(lambda x: x.cumsum())

# Function to accept or reject transaction
def newtransaction(id, amount):
    df_new = df.copy()
    df_new = df_new[df_new['user_id'] == id]
    count_average = 0
    result = ""
    df_new.sort_values('date', inplace=True)
    df_new['out_average'] = ""
    tamanho_df = df_new.shape[0]
    avg_spent = df_new.loc[df_new.index[-1], 'mean_amount']
    fraud_score = df_new.loc[df_new.index[-1], 'fraud_score']
    mean_score = df_new.loc[df_new.index[-1], 'mean_score']
    if tamanho_df != 0:
        mean_score = (mean_score / tamanho_df) * 100
    for index, row in df_new.iterrows():
        if row['over_mean'] == 1:
            count_average += 1
        else:
            count_average = 0 
        df_new.loc[index, 'out_average'] = count_average
    df_new['time_diff'] = df_new['date'].diff(periods=3).dt.total_seconds() / 60
    many_transactions = df_new.loc[df_new.index[-1], 'time_diff']
    out_average = df_new.loc[df_new.index[-1], 'out_average']
    if avg_spent != 0:
        amount_exceed = (amount / avg_spent) * 100  
    if fraud_score > 1:
        result = "Transaction denied due to high frauds detected for this user."
    if tamanho_df >= 5:      
      if mean_score >= 25:
        result = "Transaction denied due to suspicious amount in a row."    
    elif out_average >= 3:
        result = "Transaction denied due to past suspicious transactions above average spending for this user." 
    elif many_transactions <= 5:
        result = "Transaction denied due to too many transactions in a row."         
    elif amount_exceed >= 120:
        result = "Transaction denied due to suspicious amount in this transaction." 
    else:
        result = "Transaction approved." 

    print(result)

# Function to get user's fraud score
def fraudscore(id):
    df_new = df.copy()
    df_new = df_new[df_new['user_id'] == id]
    fraud_score = df_new.loc[df_new.index[-1], 'fraud_score']
    tamanho_df = df_new.shape[0]
    score = round((fraud_score/tamanho_df) * 100, 2)
    print("")
    print ("Calculating Fraud Score for this user...")
    time.sleep(2)
    print("")
    print(f"User {id} has {score}% of fraud detection in {tamanho_df} analyzed transactions.")

# Function to get key performance indicator
def kpi():
    fraudscore = df['has_cbk'].sum()
    tamanho_df = df.shape[0]
    df.sort_values('date', inplace=True)
    primeira_data = df['date'].min()
    ultima_data = df['date'].max()
    diferenca_em_dias = (ultima_data - primeira_data).days
    kpiscore = round((fraudscore/tamanho_df) * 100, 2)
    print ("Calculating KPI...")
    time.sleep(2)
    print("")
    print(f"{kpiscore}% of Fraud Score in the last {diferenca_em_dias} days.")
    print("")

# User interface loop
while True:
    os.system("cls")
    print("=================================")
    print("Welcome to FraudGuard")
    print("=================================")
    print("What would you like to do?")
    print("0 - Analyze New Transaction | 1 - Check User Fraud Score | 2 - Check Fraud KPI")
    op = int(input())

    os.system("cls")

    if op == 0:
        op02 = int(input("Enter user_id: "))        
        op03 = int(input("Enter transaction_amount: "))
        try:
            newtransaction(op02, op03) 
        except:
            print("Error: User id not found.")    

    elif op == 1:
        op04 = int(input("Enter user_id: "))
        try:
            fraudscore(op04)
        except:
            print("Error: User id not found.") 

    elif op == 2:
        kpi()

    print("\nAnalysis completed successfully!")
    print("===============================")
    print("What would you like to do next?")
    print("0 - NEW QUERY | 1 - EXIT")
    if float(input()) == 1:
        os.system("cls")
        break 
