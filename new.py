import numpy as np
import pandas as pd


def main():
	df = pd.read_csv('raw_data_joined.csv', index_col = None)
	df['Order_Date'] = pd.to_datetime(df['Order_Date'])
	df['Payment_Date'] = pd.to_datetime(df['Payment_Date'])
	list_year = pd.unique(df['Order_Date'].dt.year).tolist()
	df['Financial_Year_End'] = 0
	for year in list_year:
		future_year = pd.to_datetime('31/03/' + str(year + 1))
		current_year = pd.to_datetime('31/03/' + str(year))
		df.loc[(df['Order_Date'].dt.month > 3) & (df['Order_Date'].dt.year == year), 'Financial_Year_End'] = future_year
		df.loc[(df['Order_Date'].dt.month <= 3) & (df['Order_Date'].dt.year == year), 'Financial_Year_End'] = current_year  
	df['Financial_Year_End'], df['Order_Date'] = pd.to_datetime(df['Financial_Year_End']), pd.to_datetime(df['Order_Date'])
	df['Days_to_End'] = df['Financial_Year_End'] - df['Order_Date']
	df['Days_to_End'] = df['Days_to_End'].dt.days
	print(df.shape)
	df = df.loc[df['Days_to_End'] < 365,:]
	print(df.shape)
	df['Account_Receivable'] = np.nan
	for year in list_year:
		df.loc[(df['Order_Date'] <= df['Financial_Year_End']), 'Account_Receivable'] = df['outstanding_amount']
	df_agg = df.groupby('Financial_Year_End').agg({'Account_Receivable': 'sum', 'order_amount': 'sum'})
	df['prop_loss'] = 0
	df.to_csv('temp.csv')
	df.loc[(df['Financial_Year_End'] >= df['Order_Date']) & (df['Financial_Year_End'] >= df['Payment_Date']), 'prop_loss'] = df['Account_Receivable']/df['order_amount']
	df.loc[(df['Financial_Year_End'] >= df['Order_Date']) & (df['Financial_Year_End'] < df['Payment_Date']), 'prop_loss'] = df['payment_amount']/df['payment_amount']
	df['loss_30'], df['loss_60'], df['loss_120'], df['loss_180'], df['loss_more'] = np.nan, np.nan, np.nan, np.nan, np.nan
	df.loc[df['Days_to_End'] <= 30, 'loss_30'] = df['prop_loss']
	df.loc[(df['Days_to_End'] > 30) & (df['Days_to_End'] <= 60), 'loss_60'] = df['prop_loss']
	df.loc[(df['Days_to_End'] > 60) & (df['Days_to_End'] <= 120), 'loss_120'] = df['prop_loss'] 
	df.loc[(df['Days_to_End'] > 120) & (df['Days_to_End'] <= 180), 'loss_180'] = df['prop_loss'] 
	df.loc[df['Days_to_End'] > 180, 'loss_more'] = df['prop_loss'] 
	df_result = df.groupby('Financial_Year_End').agg({'order_amount': 'sum', 'Account_Receivable': 'sum', 'loss_30': 'mean', 'loss_60': 'mean', 'loss_120': 'mean', 'loss_180': 'mean', 'loss_more': 'mean', 'prop_loss': 'mean'})
	df_result.to_csv('result.csv')
	print(df_result)



if __name__ == '__main__':
	main()
