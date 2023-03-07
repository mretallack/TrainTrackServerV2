
run:
	. venv/bin/activate && python3 main.py 

setup:
	python3 -m venv venv
	. venv/bin/activate && pip3 install -r requirements.txt
	
	