#!/bin/bash
cat > terraform.tfvars << EOF
owners = ["user:admin@example.com"]
billing_account_id = "<123dd456789s>"
kaggle_username = "<YOUR_KAGGLE_USERNAME>"
kaggle_key = "<YOUR_KAGGLE_KEY>"
EOF