<div align="center">

# PulseAI - AI-Powered Newsletter Platform

<div align="center">

![Python](https://img.shields.io/badge/Python-3.11-blue?style=for-the-badge&logo=python&logoColor=white)
![React](https://img.shields.io/badge/React-18-61DAFB?style=for-the-badge&logo=react&logoColor=black)
![Google Cloud](https://img.shields.io/badge/Google%20Cloud-Platform-4285F4?style=for-the-badge&logo=google-cloud&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=apache-airflow&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-green?style=for-the-badge)

**Aggregating, classifying, and summarizing AI news from multiple sources into personalized newsletters.**

**🌐 [View Live Demo →](https://pulseai-insights-mcj3qs22vq-uc.a.run.app/)**

[Features](#features) • [Architecture](#architecture) • [Getting Started](#getting-started) • [Documentation](#bigquery-schema)

</div>

---

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Environment Variables](#environment-variables)
  - [Local Development](#local-development)
  - [Airflow Deployment](#airflow-deployment-astronomer)
- [Data Pipeline Schedules](#data-pipeline-schedules)
- [ML Classification Categories](#ml-classification-categories)
- [API Endpoints](#api-endpoints-react-backend)
- [BigQuery Schema](#bigquery-schema)
- [License](#license)
- [Acknowledgments](#acknowledgments)

---

## Overview

PulseAI automates the entire content pipeline from extraction to delivery:

```
Extract → Classify → Summarize → Generate → Deliver
```

1. **Extract**: Pull content from arXiv API, Reddit API, and GNews API
2. **Classify**: Tag content using ML (DistilBART-MNLI zero-shot classification)
3. **Summarize**: Generate per-tag summaries with Vertex AI Gemini 2.5 Flash
4. **Generate**: Create combined weekly newsletters synthesizing all sources
5. **Deliver**: Present via React web app and Streamlit analytics dashboard

## Features

- **Automated ETL Pipelines**: Daily extraction from arXiv, Reddit (r/MachineLearning), and GNews
- **ML Classification**: Zero-shot classification into 9 business categories using HuggingFace DistilBART-MNLI
- **AI Summarization**: Vertex AI Gemini 2.5 Flash generates high-level and detailed summaries
- **Semantic Search**: BigQuery ML embeddings for content discovery
- **Analytics Dashboard**: Streamlit-powered visualization and exploration
- **React Frontend**: Modern web interface for newsletter consumption

## Architecture

```
+-----------------------------------------------------------------------------+
|                              DATA SOURCES                                    |
+----------------+----------------+-------------------------------------------+
|    arXiv API   |   Reddit API   |              GNews API                    |
|  (cs.AI/LG/CL) |r/MachineLearning|         (AI news articles)               |
+-------+--------+-------+--------+-------------------+-----------------------+
        |                |                            |
        v                v                            v
+-----------------------------------------------------------------------------+
|                         EXTRACTION LAYER                                     |
|                    Google Cloud Functions (Gen 2)                            |
+----------------+----------------+-------------------------------------------+
| extract-arxiv  | extract-reddit |           gnews-extract                   |
+-------+--------+-------+--------+-------------------+-----------------------+
        |                |                            |
        v                v                            v
+-----------------------------------------------------------------------------+
|                          STORAGE LAYER                                       |
+------------------------------------+----------------------------------------+
|       Google Cloud Storage         |            BigQuery                     |
|    (raw JSON/NDJSON staging)       |      (raw_arxiv, raw_reddit,           |
|                                    |       raw_gnews datasets)              |
+------------------------------------+----------------------------------------+
        |                                             |
        v                                             v
+-----------------------------------------------------------------------------+
|                       TRANSFORMATION LAYER                                   |
|                    Google Cloud Functions                                    |
+----------------+----------------+-------------------------------------------+
| transform-arxiv| transform-reddit|         transform-gnews                  |
+-------+--------+-------+--------+-------------------+-----------------------+
        |                |                            |
        v                v                            v
+-----------------------------------------------------------------------------+
|                    DIMENSIONAL MODEL (pulseai_main_db)                       |
+-----------------------------+-----------------------------------------------+
|         DIMENSIONS          |                  FACTS                         |
|  - news_sources             |  - news_articles                              |
|  - authors                  |  - reddit_posts                               |
|  - calendar_date            |  - arxiv_papers                               |
|  - arxiv_categories         |  - paper_authors (bridge)                     |
+-----------------------------+-----------------------------------------------+
        |                                             |
        v                                             v
+-----------------------------------------------------------------------------+
|                      ML CLASSIFICATION LAYER                                 |
|              HuggingFace DistilBART-MNLI (Zero-Shot)                         |
+----------------+----------------+-------------------------------------------+
|arxiv_papers    |reddit_posts    |      news_articles_tagged                 |
|_tagged         |_tagged         |                                           |
+-------+--------+-------+--------+-------------------+-----------------------+
        |                |                            |
        v                v                            v
+-----------------------------------------------------------------------------+
|                      SUMMARIZATION LAYER                                     |
|                   Vertex AI Gemini 2.5 Flash                                 |
+----------------+----------------+-------------------------------------------+
|arxiv_tag       |reddit_tag      |   news_tagged_summaries                   |
|_summaries      |_summaries      |                                           |
+-------+--------+-------+--------+-------------------+-----------------------+
        |                |                            |
        +----------------+----------------------------+
                         |
                         v
+-----------------------------------------------------------------------------+
|                    COMBINED NEWSLETTER GENERATION                            |
|                      combined_newsletter table                               |
+-----------------------------------------------------------------------------+
                         |
                         v
+-----------------------------------------------------------------------------+
|                       PRESENTATION LAYER                                     |
+------------------------------------+----------------------------------------+
|          React Web App             |        Streamlit Dashboard              |
|      (react_app/ directory)        |           (app.py)                      |
|   - Newsletter consumption         |   - Analytics & visualization          |
|   - Search & filtering             |   - Data exploration                   |
|   - Trending topics                |   - Tag distribution                   |
+------------------------------------+----------------------------------------+
```

## Tech Stack

| Layer | Technology |
|-------|------------|
| Frontend | React 18, Vite, TailwindCSS, shadcn/ui |
| Backend API | Node.js, Express.js |
| Analytics | Streamlit, Plotly |
| Orchestration | Apache Airflow (Astronomer) |
| Data Warehouse | Google BigQuery |
| Object Storage | Google Cloud Storage |
| Serverless Functions | Google Cloud Functions (Gen 2) |
| ML Classification | HuggingFace Transformers (DistilBART-MNLI) |
| AI Summarization | Vertex AI Gemini 2.5 Flash |
| Language | Python 3.11 |

## Project Structure

```
PulseAI-Newsletter/
├── .astro/                    # Astronomer Airflow configuration
├── dags/                      # Airflow DAG definitions
│   ├── arxiv_etl_dag.py       # arXiv extraction pipeline
│   ├── arxiv_ml_dag.py        # arXiv ML tagging pipeline
│   ├── gnews_etl_dag.py       # GNews extraction pipeline
│   ├── gnews_ml_dag.py        # GNews ML tagging pipeline
│   ├── reddit_etl_dag.py      # Reddit extraction pipeline
│   ├── reddit_ml_dag.py       # Reddit ML tagging pipeline
│   ├── pulseai_main_transformation_dag.py  # Dimensional model transforms
│   ├── source_newsletters_dag.py           # Per-source summarization
│   └── combined_newsletter_dag.py          # Combined newsletter generation
├── functions/                 # Google Cloud Functions
│   ├── extract-arxiv-final/   # arXiv API extraction
│   ├── extract-reddit-final/  # Reddit API extraction
│   ├── gnews-final-deploymeny/# GNews API extraction
│   ├── load-arxiv-final/      # arXiv BigQuery loading
│   ├── load-reddit-final/     # Reddit BigQuery loading
│   ├── load-gnews/            # GNews BigQuery loading
│   ├── setup-arxiv-schema/    # arXiv schema setup
│   ├── setup-reddit-final/    # Reddit schema setup
│   ├── setup-gnews-schema/    # GNews schema setup
│   ├── setup-main-db-schema/  # Dimensional model schema
│   ├── transform-arxiv/       # arXiv transformations
│   ├── transform-reddit/      # Reddit transformations
│   ├── transform-gnews/       # GNews transformations
│   ├── huggingface-fb-mnli/   # ML classification service
│   ├── arxiv-summary/         # arXiv summary table setup
│   ├── arxiv-llm/             # arXiv LLM summarization
│   ├── gnews-weekly-summary-with-category/  # GNews summarization
│   ├── reddit_create_tag_summaries_table/   # Reddit summary table
│   ├── summarize_reddit_by_tag/             # Reddit summarization
│   ├── create-combined-newsletter-table/    # Combined table setup
│   └── generate-combined-newsletter/        # Combined newsletter gen
├── react_app/                 # React frontend application (see below)
├── Semantic Search/           # BigQuery semantic search implementations
│   ├── app_arxiv_semantic_search.py
│   └── app_gnews_semantic_search.py
├── Gen AI evaluations/        # Prompt evaluation framework
│   └── Prompt_eval_framework.ipynb
├── app.py                     # Streamlit analytics dashboard
├── Dockerfile                 # Astronomer Airflow container
├── requirements.txt           # Python dependencies
├── packages.txt               # System packages
└── LICENSE                    # MIT License
```

### React Frontend (react_app/)

The `react_app/` directory contains the full-stack React frontend application for newsletter consumption. Due to its size, the complete file tree is not shown above.

**Key components:**
- `src/` - React frontend source (components, pages, hooks, types)
- `backend/` - Express.js API server with BigQuery integration
- Docker and Kubernetes deployment configurations
- GCP deployment scripts (Cloud Run, GKE, App Engine)

**Frontend features:**
- Modern UI with React 18, TailwindCSS, and shadcn/ui
- Interactive data visualizations (Recharts, @visx)
- Word cloud visualizations
- Advanced search and filtering
- Responsive design

See `react_app/README.md` for detailed frontend documentation, setup instructions, and deployment guides.

## Getting Started

### Prerequisites

- Python 3.11+
- Node.js v20+ (for React frontend)
- Google Cloud Platform account with:
  - BigQuery API enabled
  - Cloud Functions API enabled
  - Cloud Storage API enabled
  - Vertex AI API enabled
- Astronomer account (for Airflow deployment)
- API keys for Reddit and GNews

### Environment Variables

```bash
# GCP Configuration
GOOGLE_CLOUD_PROJECT=your-project-id
BIGQUERY_DATASET=pulseai_main_db
GOOGLE_APPLICATION_CREDENTIALS=./service-account-key.json

# API Keys
REDDIT_CLIENT_ID=your-reddit-client-id
REDDIT_CLIENT_SECRET=your-reddit-client-secret
GNEWS_API_KEY=your-gnews-api-key
```

### Local Development

1. Clone the repository:
```bash
git clone https://github.com/BrendanWilcox/PulseAI-Newsletter.git
cd PulseAI-Newsletter
```

2. Install Python dependencies:
```bash
pip install -r requirements.txt
```

3. Set up the React frontend:
```bash
cd react_app
npm install
npm run dev
```

4. Run the Streamlit dashboard:
```bash
streamlit run app.py
```

### Airflow Deployment (Astronomer)

```bash
astro dev start
```

## Data Pipeline Schedules

| Pipeline | Schedule | Description |
|----------|----------|-------------|
| ETL Pipelines | Daily | Extract from arXiv, Reddit, GNews |
| Transformations | 1 AM UTC | Transform raw data to dimensional model |
| ML Tagging | 2 AM UTC | Classify content with DistilBART-MNLI |
| Source Summaries | Saturday midnight | Generate per-source tag summaries |
| Combined Newsletter | Saturday 3 AM EST | Synthesize all sources into newsletter |

## ML Classification Categories

Content is classified into 9 business-relevant categories:

| Category | Description |
|----------|-------------|
| **Tech** | Technology and technical topics |
| **Strategy** | Business strategy and planning |
| **Marketing** | Marketing and advertising |
| **Sales** | Sales and revenue |
| **Finance** | Financial topics |
| **HR** | Human resources and talent |
| **Supply Chain** | Operations and logistics |
| **Customer Experience** | CX and support |
| **Other** | Miscellaneous topics |

## API Endpoints (React Backend)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/health` | GET | Health check |
| `/api/newsletters` | GET | Get all newsletters |
| `/api/newsletters/:id` | GET | Get newsletter by ID |
| `/api/stats` | GET | Get statistics |
| `/api/tags` | GET | Get all tags |
| `/api/search` | GET | Search newsletters |
| `/api/trending` | GET | Get trending topics |
| `/api/wordcloud` | GET | Get word cloud data |

## BigQuery Schema

### Dimensions
| Table | Description |
|-------|-------------|
| `news_sources` | News source metadata |
| `authors` | Author information |
| `calendar_date` | Date dimension |
| `arxiv_categories` | arXiv category codes |

### Facts
| Table | Description |
|-------|-------------|
| `news_articles` | GNews articles |
| `reddit_posts` | Reddit posts |
| `arxiv_papers` | arXiv papers |
| `paper_authors` | Paper-author relationships (bridge) |

### Tagged Tables
| Table | Description |
|-------|-------------|
| `news_articles_tagged` | Articles with ML tags |
| `reddit_posts_tagged` | Posts with ML tags |
| `arxiv_papers_tagged` | Papers with ML tags |

### Summaries
| Table | Description |
|-------|-------------|
| `news_tagged_summaries` | GNews tag summaries |
| `reddit_tag_summaries` | Reddit tag summaries |
| `arxiv_tag_summaries` | arXiv tag summaries |
| `combined_newsletter` | Combined weekly newsletters |

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Original project by [divyam-rana](https://github.com/divyam-rana/PulseAI)
- Built with Google Cloud Platform, HuggingFace, and Vertex AI technologies

---

<div align="center">

**[⬆ Back to Top](#pulseai---ai-powered-newsletter-platform)**

</div>
