import argparse, yaml
from treldev.awsutils import AthenaURI, S3URI
from treldev import get_args

if __name__ == '__main__':
    args = get_args()
    schedule_instance_ts = args['schedule_instance_ts']

    output_s3 = S3URI(args['outputs']['churn_features'][0]['uri'])
    output_ath = AthenaURI(args['outputs']['churn_features_athena'][0]['uri'])

    customers_ath = AthenaURI(args['inputs']['raw.Customers'][0]['uri'])
    subscriptions_ath = AthenaURI(args['inputs']['raw.Subscriptions'][0]['uri'])
    billing_ath = AthenaURI(args['inputs']['raw.Billing'][0]['uri'])
    customer_interactions_ath = AthenaURI(args['inputs']['raw.CustomerInteractions'][0]['uri'])
    support_tickets_ath = AthenaURI(args['inputs']['raw.SupportTickets'][0]['uri'])
    
    output_ath.save_sql_results(output_s3.uri, f"""
with
Customers as (select * from {customers_ath.path})
,Subscriptions as (select * from {subscriptions_ath.path})
,Billing as (select * from {billing_ath.path})
,CustomerInteractions as (select * from {customer_interactions_ath.path})
,SupportTickets as (select * from {support_tickets_ath.path})

SELECT 
  c.CustomerID,
  SUM(cast(b.Amount as real)) AS total_billed_amount,
  COUNT(DISTINCT ci.InteractionID) AS total_interactions,
  COUNT(DISTINCT st.TicketID) AS total_support_tickets,
  (CASE 
    WHEN s.Status = 'Active' THEN 0
    ELSE 1
  END) AS churn
FROM 
  Customers c
JOIN 
  Subscriptions s ON c.CustomerID = s.CustomerID
JOIN 
  Billing b ON c.CustomerID = b.CustomerID
LEFT JOIN 
  CustomerInteractions ci ON c.CustomerID = ci.CustomerID
LEFT JOIN 
  SupportTickets st ON c.CustomerID = st.CustomerID
GROUP BY 
  c.CustomerID, s.SubscriptionStatus;
""")
