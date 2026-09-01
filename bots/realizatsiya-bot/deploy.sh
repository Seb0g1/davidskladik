#!/bin/bash
set -e

SERVER=5.129.238.210
REMOTE_DIR=/opt/bots/realizatsiya-bot
BOT_NAME=realizatsiya-bot

echo "Deploying $BOT_NAME to $SERVER..."

ssh root@$SERVER "mkdir -p $REMOTE_DIR/data $REMOTE_DIR/logs"

rsync -avz --exclude node_modules --exclude .git --exclude logs . root@$SERVER:$REMOTE_DIR/

ssh root@$SERVER "
  cd $REMOTE_DIR
  npm install --production
  if pm2 list | grep -q $BOT_NAME; then
    pm2 restart $BOT_NAME
  else
    pm2 start ecosystem.config.cjs --env production
  fi
  pm2 save
  echo 'Deploy complete'
"

echo "Done!"
