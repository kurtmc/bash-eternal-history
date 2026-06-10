# bash-eternal-history

This is a simple filesystem in userspace that contains a single file
`.bash_eternal_history` created for the purpose of storing your bash history
indefinitely and across many systems. The filesystem makes use of [AWS
DynamoDB](https://aws.amazon.com/dynamodb) to store every command entered in
bash which may be appended to by multiple sessions simultaneously.

> **Security note:** shell history routinely contains secrets (tokens,
> passwords pasted into commands, connection strings). Every line is stored in
> the DynamoDB table, so anyone with read access to that table can read your
> history. Scope access to the table tightly and avoid pasting secrets into
> your shell.

## Setup

1. Download the latest release from GitHub: https://github.com/kurtmc/bash-eternal-history/releases/latest

2. Create an IAM identity for the tool with least privilege. It only needs
access to the history table:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "dynamodb:DescribeTable",
        "dynamodb:CreateTable",
        "dynamodb:PutItem",
        "dynamodb:Scan"
      ],
      "Resource": "arn:aws:dynamodb:*:YOUR_ACCOUNT_ID:table/bash-eternal-history"
    }
  ]
}
```
If you create the table yourself, you can drop `dynamodb:CreateTable`.

3. Configure credentials. Prefer a credentials file or an instance role over
embedding keys in the systemd unit — `Environment=` values are visible to
anyone who can read the unit file or run `systemctl show`. For example, add a
profile to `~/.aws/credentials`:
```ini
[bash-eternal-history]
aws_access_key_id = XXXXXXXXXXXXXXXXXXXX
aws_secret_access_key = XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
```
On EC2, skip the profile entirely and attach an instance role instead.

4. Create a systemd service to automatically mount the filesystem, example
configuration:
```
[Unit]
Description=Mounts bash eternal history

[Service]
Type=simple
Environment=AWS_PROFILE=bash-eternal-history
Environment=AWS_REGION=ap-southeast-2
ExecStartPre=/usr/bin/mkdir -p %h/.bash-eternal-history-fuse
ExecStart=/opt/bash-eternal-history/bash-eternal-history %h/.bash-eternal-history-fuse
ExecStop=/usr/bin/umount %h/.bash-eternal-history-fuse

[Install]
WantedBy=default.target
```

5. Create a symlink to `.bash-eternal-history-fuse`
```
ln -s -f $HOME/.bash-eternal-history-fuse/.bash_eternal_history $HOME/.bash_eternal_history
```

6. Configure bash history in `~/.bashrc`, see: https://stackoverflow.com/a/19533853
```
export HISTFILESIZE=
export HISTSIZE=
export HISTTIMEFORMAT="[%F %T] "
export HISTFILE=~/.bash_eternal_history
PROMPT_COMMAND="history -a; $PROMPT_COMMAND"
```

7. Profit.

## Configuration

The daemon is configured through environment variables:

| Variable | Default | Description |
| --- | --- | --- |
| `DYNAMODB_TABLE_NAME` | `bash-eternal-history` | DynamoDB table used to store history. Created automatically if it does not exist. |
| `READ_CONTENT_TIMEOUT` | `15s` | Timeout for loading the history content from DynamoDB. |
| `CONTENT_CACHE_TTL` | `5m` | How long loaded history is served before it is refreshed in the background, picking up commands written by other machines. Set to `0` to load only once per mount. |
