#!/usr/bin/env bash
set -o errexit
pip install -r requirements.txt
python -m textblob.download_corpora lite
python -c "import nltk; nltk.download('punkt_tab', quiet=True); nltk.download('wordnet', quiet=True); nltk.download('stopwords', quiet=True); nltk.download('averaged_perceptron_tagger_eng', quiet=True)"
