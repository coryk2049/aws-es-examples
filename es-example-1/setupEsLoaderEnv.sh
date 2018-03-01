#!/bin/bash
VIRTUAL_ENV=venv_esloader
rm -rf ${VIRTUAL_ENV}
virtualenv -p /usr/bin/python2.7 ${VIRTUAL_ENV}
source ${VIRTUAL_ENV}/bin/activate
pip install elasticsearch
pip install requests-aws4auth
pip install requests
pip install -U requests
pip show elasticsearch
pip show requests
#pip install -r requirements.txt
#deactivate
