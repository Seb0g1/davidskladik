const { Client } = require("ssh2");

const conn = new Client();
conn.on("ready", () => {
  // Read env, login, test orders
  conn.exec(
    "U=$(grep ^APP_USER= /var/www/davidsklad/davidskladik/.env | cut -d= -f2-)" +
    " && P=$(grep ^APP_PASSWORD= /var/www/davidsklad/davidskladik/.env | cut -d= -f2-)" +
    " && curl -sk -c /tmp/sess.txt -X POST http://localhost:3000/api/auth/login" +
    " -H 'Content-Type: application/json' -d \"{\\\"username\\\":\\\"$U\\\",\\\"password\\\":\\\"$P\\\"}\"" +
    " -w '\\nLOGIN:%{http_code}'" +
    " && curl -sk -b /tmp/sess.txt 'http://localhost:3000/api/shop/admin/orders?pageSize=5'" +
    " -w '\\nORDERS:%{http_code}'",
    (err, stream) => {
      let out = "";
      stream.on("data", (d) => (out += d));
      stream.stderr.on("data", (d) => (out += d));
      stream.on("close", () => {
        try {
          // Extract JSON before status codes
          const parts = out.split("\n");
          parts.forEach((line) => {
            if (line.startsWith("LOGIN:") || line.startsWith("ORDERS:")) {
              console.log(line);
            } else if (line.length > 5) {
              try {
                const d = JSON.parse(line);
                if (d.orders) {
                  console.log("Orders count:", d.orders.length, "| Total:", d.total);
                  d.orders.forEach((o) =>
                    console.log(" ", o.id, o.status, o.totalRub + "₽", JSON.stringify(o.delivery).slice(0, 80))
                  );
                } else {
                  console.log(line.slice(0, 200));
                }
              } catch { console.log(line.slice(0, 100)); }
            }
          });
        } catch { console.log(out.slice(0, 500)); }
        conn.end();
      });
    }
  );
}).connect({
  host: "81.17.154.153",
  username: "root",
  password: "pm^e7-jVL_gAyM",
  readyTimeout: 20000,
});
