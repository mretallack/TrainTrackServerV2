
PYTHON=python3.11

run: venv/bin/activate
	. venv/bin/activate && ${PYTHON} -u main.py 


venv/bin/activate:
	${PYTHON} -m venv venv
	. venv/bin/activate && ${PYTHON} -m pip install -r requirements.txt
	

