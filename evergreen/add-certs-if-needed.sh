#!/usr/bin/env bash

set -o xtrace   # Write all commands first to stderr
set -o errexit  # Exit the script with an error if any of the commands fail

# Supported/used environment variables:
#     SSL                     Set to enable SSL. Values are "ssl" / "nossl" (default)
#     OCSP_TLS_SHOULD_SUCCEED Set to test OCSP. Values are true/false/nil
#     OCSP_ALGORITHM          Set to test OCSP. Values are rsa/ecdsa/nil
#     OS                      Set to access operating system

SSL=${SSL:-nossl}
OCSP_TLS_SHOULD_SUCCEED=${OCSP_TLS_SHOULD_SUCCEED:-nil}
OCSP_ALGORITHM=${OCSP_ALGORITHM:-nil}

if [[ "$SSL" != "ssl" ]]; then
  exit 0
fi

function make_trusted() {
  echo "CA.pem certificate $1"
  if [[ "$OS" =~ Windows|windows ]]; then
    # makes the client.pem trusted
    certutil.exe -addstore "Root" $1
  elif  [[ "$OS" =~ Ubuntu|ubuntu ]]; then
    # makes the client.pem trusted
    # note: .crt is the equivalent format as .pem, but we need to make this renaming because update-ca-certificates supports only .crt
    sudo cp -f $1 /usr/local/share/ca-certificates/ca.crt
    sudo update-ca-certificates
  else
    # mac OS, the same trick as for above ubuntu step
    sudo cp -f $1 ~/ca.crt
    sudo security add-trusted-cert -d -r trustRoot -k /Library/Keychains/System.keychain ~/ca.crt
    #sudo security delete-certificate -c "<name of existing certificate>"
  fi
}

make_trusted ${DRIVERS_TOOLS}/.evergreen/x509gen/ca.pem

if [[ "$OCSP_TLS_SHOULD_SUCCEED" != "nil" && "$OCSP_ALGORITHM" != "nil" ]]; then
  make_trusted ${DRIVERS_TOOLS}/.evergreen/ocsp/${OCSP_ALGORITHM}/ca.pem
fi
