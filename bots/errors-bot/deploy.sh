#!/bin/bash
set -e

SERVER=5.129.238.210
REMOTE_DIR=/opt/bots/errors-bot

echo "Deploying errors-bot to $SERVER..."

# Sync files excluding node_modules and logs
rsync -avz --exclude node_modules --exclude logs --exclude .env \
  . root@$SERVER:$REMOTE_DIR/

# Install deps and restart via PM2
ssh root@$SERVER "
  cd $REMOTE_DIR
  npm install --production
  pm2 restart errors-bot 2>/dev/null || pm2 start ecosystem.config.cjs --env production
  pm2 save
  echo 'errors-bot deployed successfully'
"

echo "Done. Check with: ssh root@$SERVER 'pm2 status'"
