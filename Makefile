


run:
	. venv/bin/activate && python3.11 -u main.py 

setup:
	python3.11 -m venv venv
	. venv/bin/activate && pip3 install -r requirements.txt
	
	
