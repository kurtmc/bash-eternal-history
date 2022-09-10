# bash-eternal-history

This is a simple filesystem in userspace that contains a single file
`.bash_eternal_history` created for the purpose of storing your bash history
indefinitely and across many systems. The filesystem makes use of [AWS
DynamoDB](https://aws.amazon.com/dynamodb) to store every command entered in
bash which may be appended to by multiple sessions simultaneously.

## Setup

1. Download the latest release from GitHub: https://github.com/kurtmc/bash-eternal-history/releases/latest
2. Create systemd service to automatically mount the filesystem, example configuration:
```
[Unit]
Description=Mounts bash eternal history

[Service]
Type=simple
Environment=AWS_ACCESS_KEY_ID=XXXXXXXXXXXXXXXXXXXX
Environment=AWS_SECRET_ACCESS_KEY=XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
Environment=AWS_REGION=ap-southeast-2
ExecStartPre=/usr/bin/mkdir -p %h/.bash-eternal-history-fuse
ExecStart=/opt/bash-eternal-history/bash-eternal-history %h/.bash-eternal-history-fuse

[Install]
WantedBy=default.target
```
3. Create a symlink to `.bash-eternal-history-fuse`
```
ln -s -f $HOME/.bash-eternal-history-fuse/.bash_eternal_history $HOME/.bash_eternal_history
```
4. Configure bash history in `~/.bashrc`, see: https://stackoverflow.com/a/19533853
```
export HISTFILESIZE=
export HISTSIZE=
export HISTTIMEFORMAT="[%F %T] "
export HISTFILE=~/.bash_eternal_history
PROMPT_COMMAND="history -a; $PROMPT_COMMAND"
```

5. Profit.
