#!/bin/bash

# Don't trace since the URI contains a password that shouldn't show up in the logs
set -o errexit  # Exit the script with error if any of the commands fail

# Supported/used environment variables:
#       AUTH_HOST             Set the hostname of a key distribution center (KDC)
#       AUTH_GSSAPI           Set the GSSAPI credentials, including a user principal/password to use to connect to AUTH_HOST server via GSSAPI authentication mechanism

############################################
#            Main Program                  #
############################################
echo "Running GSSAPI authentication tests"

# Provision the correct connection string and set up SSL if needed
for var in TMP TEMP NUGET_PACKAGES NUGET_HTTP_CACHE_PATH APPDATA; do setx $var z:\\data\\tmp; export $var=z:\\data\\tmp; done

if [ "Windows_NT" = "$OS" ]; then
  cmd /c "REG ADD HKLM\SYSTEM\ControlSet001\Control\Lsa\Kerberos\Domains\LDAPTEST.10GEN.CC /v KdcNames /d ldaptest.10gen.cc /t REG_MULTI_SZ /f"
  echo "LDAPTEST.10GEN.CC registry has been added"
  
  cmd /c "REG ADD HKLM\SYSTEM\ControlSet001\Control\Lsa\Kerberos\Domains\LDAPTEST2.10GEN.CC /v KdcNames /d ldaptest.10gen.cc /t REG_MULTI_SZ /f"
  echo "LDAPTEST2.10GEN.CC registry has been added"
fi;

export EXPLICIT=true

# Arguments for AUTH_GSSAPI + AUTH_HOST
export MONGODB_URI="mongodb://${AUTH_GSSAPI}@${AUTH_HOST}:27017/kerberos?authMechanism=GSSAPI"

powershell.exe .\\build.ps1 -target TestGssapi
