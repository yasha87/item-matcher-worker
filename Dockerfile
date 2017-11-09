FROM python:2.7-onbuild

CMD ["python", "-u" , "item-matcher-worker.py"]
