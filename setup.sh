#!/bin/bash

# Install system dependencies for Streamlit Cloud
apt-get update
apt-get install -y poppler-utils tesseract-ocr tesseract-ocr-eng libgl1
