// Run: node scripts/gen-vapid.cjs
// Generates a fresh VAPID key pair. Add the output to .env.
// Only needed once — changing keys invalidates all existing push subscriptions.
const wp = require("web-push");
const { publicKey, privateKey } = wp.generateVAPIDKeys();
console.log("Add these to .env:\n");
console.log(`VAPID_PUBLIC_KEY=${publicKey}`);
console.log(`VAPID_PRIVATE_KEY=${privateKey}`);
