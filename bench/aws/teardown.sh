#!/bin/bash
# Terminates every instance tagged Project=lionturtle-bench and deletes the
# benchmark security group and key pair. Touches nothing else.
set -uo pipefail
R=${REGION:-us-west-2}; name=$1
dir=$(dirname "$0")
ids=$(aws ec2 describe-instances --region $R --filters Name=tag:Project,Values=lionturtle-bench Name=instance-state-name,Values=pending,running,stopping,stopped \
  --query 'Reservations[].Instances[].InstanceId' --output text)
if [ -n "$ids" ]; then
  aws ec2 terminate-instances --region $R --instance-ids $ids >/dev/null
  aws ec2 wait instance-terminated --region $R --instance-ids $ids
fi
sg=$(aws ec2 describe-security-groups --region $R --filters Name=group-name,Values="$name" Name=tag:Project,Values=lionturtle-bench --query 'SecurityGroups[0].GroupId' --output text)
[ "$sg" != "None" ] && aws ec2 delete-security-group --region $R --group-id "$sg"
aws ec2 delete-key-pair --region $R --key-name "$name" 2>/dev/null
rm -f "$dir/key.pem"
echo "remaining tagged instances:"; aws ec2 describe-instances --region $R --filters Name=tag:Project,Values=lionturtle-bench --query 'Reservations[].Instances[].[InstanceId,State.Name]' --output text
echo "remaining tagged volumes:"; aws ec2 describe-volumes --region $R --filters Name=tag:Project,Values=lionturtle-bench --query 'Volumes[].[VolumeId,State]' --output text
echo "remaining tagged spot requests:"; aws ec2 describe-spot-instance-requests --region $R --filters Name=state,Values=open,active --query 'SpotInstanceRequests[].[SpotInstanceRequestId,State,InstanceId]' --output text
