# Import libraries
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# Define global parameters
file_customer = 'customers.csv'
file_order = 'orders.csv'
file_payment = 'payments.csv'

class Aging:
	def __init__(self, file_customer, file_order, file_payment):
		self.file_customer = file_customer
		self.file_order = file_order
		self.file_payment = file_payment
		self.df_customer = pd.DataFrame()
		self.df_order = pd.DataFrame()
		self.df_payment = pd.DataFrame()
		self.df_master = pd.DataFrame()
		self.df_master_agg = pd.DataFrame()
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
		self.df_master['outstanding_amount'] = self.df_master['order_amount'] - self.df_master['payment_amount']
		self.df_master_agg = self.df_master.groupby(['OrderID', 'Payment_Date']).agg({'outstanding_amount': 'sum'})
		self.df_master_agg = self.df_master_agg.reset_index()

	def diagnostics(self):
		self.total_outstanding = self.df_master['outstanding_amount'].sum()
		self.total_order_amount = self.df_master['order_amount'].sum()
		print('Dimensionality for customer data')
		print(self.df_customer.shape)
		print('Dimensionality for order data')
		print(self.df_order.shape)
		print('Dimensionality for payment data')
		print(self.df_payment.shape)
		print('Dimensionality for aggregated data')
		print(self.df_master.shape)
		print(self.df_master.tail())
		print('Total amount outstanding: $' + '{:.2E}'.format(self.total_outstanding))
		print('Total amount outstanding as a percentage of total order amount: ' + '{:.2%}'.format(self.total_outstanding/self.total_order_amount))

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
		temp_df_master_agg = temp_df_master_agg.set_index('Payment_Date')
		temp_df_master_agg.plot()
		plt.title('Outstanding Amount vs. Payment Date')
		plt.ylabel('Outstanding Amount')
		plt.xlabel('Payment Date')
		plt.savefig('outstanding_time_series.png', dpi = 300)
		plt.close()

	def data_export(self):
		self.df_master_agg.to_csv('order_agg_data.csv')

	def exec(self):
		self.data_import()
		self.data_clean()
		self.database_op()
		self.data_clean_2()
		self.diagnostics()
		self.visualisation()
		self.data_export()




def main():
	Obj = Aging(file_customer, file_order, file_payment)
	Obj.exec()












if __name__ == '__main__':
	main()
