# Test certificates

For tests only. `server.pem` is for `localhost` and `127.0.0.1`, signed by `ca.pem`, and valid until 2126. The CA's key was deleted, so nothing else is signed by it.

```sh
openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out ca-key.pem
openssl req -x509 -new -key ca-key.pem -subj "/CN=domain test CA" -days 36500 -out ca.pem \
  -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign"
openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out server-key.pem
openssl req -new -key server-key.pem -subj "/CN=localhost" -out server.csr
printf "basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature\nextendedKeyUsage=serverAuth\nsubjectAltName=DNS:localhost,IP:127.0.0.1\n" > ext.cnf
openssl x509 -req -in server.csr -CA ca.pem -CAkey ca-key.pem -CAcreateserial -days 36500 -extfile ext.cnf -out server.pem
```
