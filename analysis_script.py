# Import libraries
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# Configure the programme
pd.set_option('display.float_format', '{:.2f}'.format)

# Define global parameters
file_customer = 'customers.csv'
file_order = 'orders.csv'
file_payment = 'payments.csv'
output_text_file = 'text_output.txt'
list_category = ['Days_30', 'Days_60', 'Days_90', 'Days_120', 'Days_More_Than_120']
list_prob = ['prob_30', 'prob_60', 'prob_90', 'prob_120', 'prob_more_than_120']

class Aging:
	def __init__(self, file_customer, file_order, file_payment, output_text_file, list_prob, list_category):
		self.file_customer = file_customer
		self.file_order = file_order
		self.file_payment = file_payment
		self.output_text_file = output_text_file
		self.list_prob = list_prob
		self.list_category = list_category
		self.df_customer = pd.DataFrame()
		self.df_order = pd.DataFrame()
		self.df_payment = pd.DataFrame()
		self.df_master = pd.DataFrame()
		self.df_master_agg = pd.DataFrame()
		self.df_prob = pd.DataFrame()
		self.total_outstanding = 0

	def data_import(self):
		self.df_customer = pd.read_csv(file_customer)
		self.df_order = pd.read_csv(file_order)
		self.df_payment = pd.read_csv(file_payment)

	def data_clean(self):
		self.df_order = self.df_order.rename(columns = {'amount': 'order_amount', 'Date': 'Order_Date'})
		self.df_payment = self.df_payment.rename(columns = {'amount': 'payment_amount', 'Date': 'Payment_Date'})

	def database_op(self):
		self.df_master = self.df_customer.merge(self.df_order, on = 'CustID', how = 'right')
		self.df_master = self.df_master.merge(self.df_payment, on = ['CustID', 'OrderID'], how = 'left')

	def data_clean_2(self):
		self.df_master['payment_amount'] = self.df_master['payment_amount'].fillna(0)
		self.df_master['Payment_Date'] = self.df_master['Payment_Date'].fillna('2099-01-01')
		self.df_master['Payment_Date'] = pd.to_datetime(self.df_master['Payment_Date'])
		self.df_master['Order_Date'] = pd.to_datetime(self.df_master['Order_Date'])
		self.df_master['outstanding_amount'] = self.df_master['order_amount'] - self.df_master['payment_amount']
		self.df_master_agg = self.df_master.groupby(['OrderID', 'Payment_Date', 'Order_Date']).agg({'outstanding_amount': 'sum', 'order_amount': 'sum'})
		self.df_master_agg = self.df_master_agg.reset_index()
		self.df_master_agg.to_csv('raw_data_joined.csv', index = False)
		self.df_master_agg = Aux.aging_var_create(self.df_master_agg, self.list_category)
		self.total_outstanding = self.df_master['outstanding_amount'].sum()
		self.total_order_amount = self.df_master['order_amount'].sum()

	def analyse(self):
		list_agg = self.list_prob + ['outstanding_amount']
		self.df_master_agg = Aux.prob_var_create(self.df_master_agg, self.list_prob, self.list_category)
		self.df_prob = self.df_master_agg.groupby(self.df_master_agg['Order_Date'].dt.year)[list_agg].mean()
		self.df_prob['prop_total_outstanding'] = self.df_prob['outstanding_amount']/self.total_outstanding
		with open(self.output_text_file, 'w') as f:
			for i in range(len(self.list_prob)):
				prob_int = [int(s) for s in Aux.char_split(self.list_prob[i]) if s.isdigit()]
				prob_int = ''.join(str(char) for char in prob_int)
				if i != len(self.list_prob) - 1:
					print('Average Percentage of Account Receivable Unpaid Before ' + str(prob_int) + ' Days: ' + '{:.2%}'.format(self.df_prob[self.list_prob[i]].mean()), file = f)
				else:
					print('Average Percentage of Account Receivable Unpaid After ' + str(prob_int) + ' Days: ' + '{:.2%}'.format(self.df_prob[self.list_prob[i]].mean()), file = f)
			print(self.df_prob, file = f)

	def diagnostics(self):
		with open('text_output.txt', 'a') as f:
			print('Dimensionality for customer data', file = f)
			print(self.df_customer.shape, file = f)
			print('Dimensionality for order data', file = f)
			print(self.df_order.shape, file = f)
			print('Dimensionality for payment data', file = f)
			print(self.df_payment.shape, file = f)
			print('Dimensionality for aggregated data', file = f)
			print(self.df_master.shape, file = f)
			print('Total amount outstanding: $' + '{:.2E}'.format(self.total_outstanding), file = f)
			print('Total amount outstanding as a percentage of total order amount: ' + '{:.2%}'.format(self.total_outstanding/self.total_order_amount), file = f)

	def visualisation(self):
		plt.figure(figsize = (16, 9))
		self.df_master_agg.loc[self.df_master_agg['outstanding_amount'] != 0, 'outstanding_amount'].hist(bins = 120)
		plt.title('Histogram of Outstanding Amount')
		plt.ylabel('Frequency')
		plt.xlabel('Outstanding Amount')
		plt.savefig('outstanding_hist.png', dpi = 300)
		plt.close()

		plt.figure(figsize = (16, 9))
		temp_df_master_agg = self.df_master_agg.loc[self.df_master_agg['Payment_Date'] != pd.to_datetime('2099-01-01'), :]
		temp_df_master_agg.set_index('Payment_Date')['outstanding_amount'].plot()
		plt.title('Outstanding Amount vs. Payment Date')
		plt.ylabel('Outstanding Amount')
		plt.xlabel('Payment Date')
		plt.savefig('outstanding_time_series.png', dpi = 300)
		plt.close()

		plt.figure(figsize = (16, 9))
		temp_df_master_agg.set_index('Payment_Date')['Delay_Days'].hist(bins = 120)
		plt.title('Histogram of Delayed Days')
		plt.ylabel('Frequency')
		plt.xlabel('Delayed_Days')
		plt.savefig('delayed_days_hist.png', dpi = 300)
		plt.close()

	def data_export(self):
		self.df_master_agg.to_csv('order_agg_data.csv')

	def exec(self):
		self.data_import()
		self.data_clean()
		self.database_op()
		self.data_clean_2()
		self.analyse()
		self.diagnostics()
		self.visualisation()
		self.data_export()

class Aux:
	def aging_var_create(df, list_category):
		df['Delay_Days'] = (df['Payment_Date'] - df['Order_Date'])/np.timedelta64(1, 'D')
		df['Days_30'], df['Days_60'], df['Days_90'], df['Days_120'], df['Days_More_Than_120'] = 0, 0, 0, 0, 0
		df.loc[df['Delay_Days'] <= 30, list_category[0]] = 1
		df.loc[(df['Delay_Days'] > 30) & (df['Delay_Days'] <= 60), list_category[1]] = 1
		df.loc[(df['Delay_Days'] > 60) & (df['Delay_Days'] <= 90), list_category[2]] = 1
		df.loc[(df['Delay_Days'] > 90) & (df['Delay_Days'] <= 120), list_category[3]] = 1
		df.loc[df['Delay_Days'] > 120, list_category[4]] = 1
		return df

	def prob_var_create(df, list_prob, list_category):
		for i in range(len(list_prob)):
			df[list_prob[i]] = (df['outstanding_amount'] * df[list_category[i]])/(df['order_amount'] * df[list_category[i]])
		return df

	def char_split(word):
		return [char for char in word]

def main():
	Obj = Aging(file_customer, file_order, file_payment, output_text_file, list_prob, list_category)
	Obj.exec()












if __name__ == '__main__':
	main()
