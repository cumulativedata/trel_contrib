import argparse, yaml
from treldev.awsutils import AthenaURI, S3URI
from treldev import get_args

if __name__ == '__main__':
    args = get_args()
    schedule_instance_ts = args['schedule_instance_ts']

    output_s3 = S3URI(args['outputs']['churn_labels'][0]['uri'])
    output_ath = AthenaURI(args['outputs']['churn_labels_athena'][0]['uri'])
    
    subscriptions_ath = AthenaURI(args['inputs']['raw.Subscriptions'][0]['uri'])
    
    output_ath.save_sql_results(output_s3.uri, f"""
with
Subscriptions as (select * from {subscriptions_ath.path})

,churned_customers AS (
  SELECT
    CustomerID,
    CASE
      WHEN Status = 'canceled' THEN 1
      WHEN Status = 'expired' THEN 1
      ELSE 0
    END AS churn_label
  FROM Subscriptions
)

SELECT
  CustomerID,
  MIN(churn_label)
FROM churned_customers
GROUP BY 1;
""")

    output_ath.validate(args, save=True, sodacore_check=True)
