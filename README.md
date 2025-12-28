Notes

~~1. openssl genrsa -out ca-key.pem 4096~~

~~2. export MSYS2_ARG_CONV_EXCL='-subj='~~
~~2. openssl req -x509 -new -nodes -key ca-key.pem -sha256 -days 3650 -out ca-cert.pem -subj="/C=US/ST=CA/L=SanFrancisco/O=ExampleOrg/OU=IT/CN=Example-CA"~~


1. Don't manually load PEM, rely on Windows Trusted Root Store
2. Force TLS 1.2 as SingleStore cloud often drops older protocols