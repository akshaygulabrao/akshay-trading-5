
rsync -avz -e "ssh -i $EC2_KEY" ec2-user@$EC2_HOST:/opt/trading_server/orders.db $ORDERS_DB_PATH
