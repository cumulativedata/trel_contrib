# Contents

This pipeline consists of 3 parts:

1. A sensor to pull the data from Salesforce
2. A job to fix titles in the Lead table
3. An export job to push the data back to Salesforce

# Step 1: Sensor

Use the sensor located [here](../../sensors/salesforce/).

# Step 2: Job to fix title

Use the registration file [trel_regis_fix_title.yml](trel_regis_fix_title.yml) to register this job. The code is present in this folder.

# Step 3: Export

The export job is located [here](../../export_jobs/salesforce/).

