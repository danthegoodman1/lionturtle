#!/bin/bash
# Launches one tagged spot instance for benchmarking. Prints its id and IP.
# Usage: launch.sh <instance-type> <subnet> <name>
set -euo pipefail
type=$1; subnet=$2; name=$3
R=${REGION:-us-west-2}
dir=$(dirname "$0")
vpc=$(aws ec2 describe-subnets --region $R --subnet-ids "$subnet" --query 'Subnets[0].VpcId' --output text)
ami=$(aws ssm get-parameter --region $R --name /aws/service/canonical/ubuntu/server/26.04/stable/current/amd64/hvm/ebs-gp3/ami-id --query Parameter.Value --output text)
tags="{Key=Project,Value=lionturtle-bench},{Key=Name,Value=$name}"
if [ ! -f "$dir/key.pem" ]; then
  aws ec2 create-key-pair --region $R --key-name "$name" --tag-specifications "ResourceType=key-pair,Tags=[$tags]" \
    --query KeyMaterial --output text > "$dir/key.pem"
  chmod 600 "$dir/key.pem"
fi
sg=$(aws ec2 describe-security-groups --region $R --filters Name=group-name,Values="$name" --query 'SecurityGroups[0].GroupId' --output text)
if [ "$sg" = "None" ]; then
  sg=$(aws ec2 create-security-group --region $R --group-name "$name" --description "lionturtle benchmark ssh" --vpc-id "$vpc" \
    --tag-specifications "ResourceType=security-group,Tags=[$tags]" --query GroupId --output text)
  aws ec2 authorize-security-group-ingress --region $R --group-id "$sg" --protocol tcp --port 22 --cidr "$(curl -s https://checkip.amazonaws.com)/32" >/dev/null
fi
id=$(aws ec2 run-instances --region $R --instance-type "$type" --image-id "$ami" --key-name "$name" \
  --security-group-ids "$sg" --subnet-id "$subnet" \
  --instance-market-options 'MarketType=spot,SpotOptions={SpotInstanceType=one-time,InstanceInterruptionBehavior=terminate}' \
  --block-device-mappings '[{"DeviceName":"/dev/sda1","Ebs":{"VolumeSize":100,"VolumeType":"gp3","DeleteOnTermination":true}}]' \
  --tag-specifications "ResourceType=instance,Tags=[$tags]" "ResourceType=volume,Tags=[$tags]" \
  --query 'Instances[0].InstanceId' --output text)
aws ec2 wait instance-running --region $R --instance-ids "$id"
ip=$(aws ec2 describe-instances --region $R --instance-ids "$id" --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
echo "$id $ip"
