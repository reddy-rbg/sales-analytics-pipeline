FROM astrocrpublic.azurecr.io/runtime:3.0-6

FROM quay.io/astronomer/astro-runtime:11.0.0

# Copy requirements and install
COPY requirements.txt .
RUN pip install -r requirements.txt