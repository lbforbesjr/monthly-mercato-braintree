import os
import pandas as pd
import numpy as np
import braintree
from datetime import datetime as dt, timedelta as td, timezone as tz
from pandas.tseries.offsets import MonthEnd
from dotenv import load_dotenv

# Utility variables
ROOT = os.path.dirname(__file__)  # Path of the script
os.chdir(ROOT)

# Load environment variables
load_dotenv()

# Configuration variables
MERCHANT_ID = os.getenv('mercato_braintree_merchant_id')
PUBLIC_KEY = os.getenv('mercato_braintree_public_key')
PRIVATE_KEY = os.getenv('mercato_braintree_private_key')
REPORTS_DIR = 'reports'

# Initialize Braintree gateway
GATEWAY = braintree.BraintreeGateway(
    braintree.Configuration(
        environment=braintree.Environment.Production,
        merchant_id=MERCHANT_ID,
        public_key=PUBLIC_KEY,
        private_key=PRIVATE_KEY
    )
)

def generate_braintree_reports(month_str=None):
    """
    Generate Braintree transaction reports for a given month.
    :param month_str: A string in 'YYYY-MM' format, default is the previous month.
    """
    # Calculate the month to generate the report for
    now = dt.now()
    if month_str is None:
        now = now - td(days=now.day)  # Set to the last day of the previous month
        month_str = now.strftime('%Y-%m')

    print(f'Generating report for {month_str}...')

    # Define the start and end dates for the month
    start_date = (dt.strptime(month_str, '%Y-%m') - MonthEnd(1) + td(1)).strftime('%m/%d/%Y 00:00')
    end_date = (dt.strptime(month_str, '%Y-%m') + MonthEnd(0)).strftime('%m/%d/%Y 23:59')

    # Query transactions from Braintree
    search_results = GATEWAY.transaction.search([
        braintree.TransactionSearch.created_at.between(start_date, end_date),
        braintree.TransactionSearch.status == 'settled'
    ])

    # Process the search results
    transactions = []
    for item in search_results.items:
        data = {
            'braintree_id': item.id,
            'order_id': item.order_id,
            'type': item.type,
            'created_at': item.created_at,
            'amount': item.amount,
            'service_fee': item.service_fee_amount,
            'custom_description': item.custom_fields['description'] if isinstance(item.custom_fields, dict) else '',
            'status': item.status,
            'payment_type': item.payment_instrument_type,
            'processor_auth_code': item.processor_authorization_code,
            'refund_id': item.refund_id,
            'settlement_batch_id': item.settlement_batch_id
        }
        transactions.append(data)

    # Create a DataFrame and apply transformations
    df = pd.DataFrame(transactions)
    df['amount'] = np.where(df['type'] == 'credit', df['amount'] * -1, df['amount'])
    df['batch_date'] = df['settlement_batch_id'].str[:10]

    # Filter refunds
    refunds = df[df['refund_id'].str.len() > 4]
    refunds = refunds[['refund_id', 'custom_description']]

    # Ensure reports directory exists
    os.makedirs(REPORTS_DIR, exist_ok=True)

    # Save the reports to CSV files
    df.to_csv(f'{REPORTS_DIR}/{month_str} braintree_api_report.csv', index=False, mode='w')
    refunds.to_csv(f'{REPORTS_DIR}/{month_str} braintree_api_report_refunds.csv', index=False, mode='w')

    print(f'{month_str} report generated successfully.')

# Run the report generator for the default month
generate_braintree_reports()

# Uncomment below to generate reports for specific months
# months_2023 = [
#     '2023-05',
#     '2023-06',
#     '2023-07',
#     '2023-08',
#     '2023-09',
# ]
# for month in months_2023:
#     generate_braintree_reports(month)