#!/bin/bash

cd /workplace/Sasree
exec uvicorn app.main:app --host 0.0.0.0 --port 8000