# 💬 AI-Powered Business Intelligence Chatbot

Turn raw business data into instant answers — no SQL, no manual dashboards, no data prep. Ask a question in plain English, get a table, a chart, and an answer.

[![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)](https://www.python.org/)
[![Streamlit](https://img.shields.io/badge/Streamlit-App-FF4B4B.svg)](https://streamlit.io/)
[![MySQL](https://img.shields.io/badge/Database-MySQL-4479A1.svg)](https://www.mysql.com/)
[![Status](https://img.shields.io/badge/Status-Active%20Development-yellow.svg)]()


---

## 📖 Overview

Most business teams sit on useful data they can't actually query — because querying it requires SQL knowledge, a data analyst, or hours spent building a dashboard. This project closes that gap.

The chatbot takes a natural-language question — *"Show me monthly sales by region"* — and turns it into a working SQL query, runs it against the connected database, and returns the result as both a table and an automatically-generated chart. No technical skill required from the end user.

---

## ✨ Features

- **🔐 Secure Access** — Gmail OAuth login. Each user gets a private ID, and full control over their own data (view, manage, or delete at any time).
- **🛠️ Hybrid Data Processing** — combines manual data cleaning with automated feature engineering behind the scenes, so raw, messy business data doesn't need to be prepped before use.
- **💬 Natural Language → SQL** — user questions are translated into SQL queries automatically, so no SQL knowledge is needed to explore the data.
- **📊 Visual Intelligence** — every query returns both a tabular result and an automatically generated visualization (chart type selected based on the query), so insights are readable at a glance.
- **⚡ Instant Results** — no manual dashboard-building or report generation; ask and get an answer in the same conversation.

---

## 🧩 How It Works

```
User Question (Natural Language)
        │
        ▼
  NLP Query Parser
        │
        ▼
  Natural Language → SQL Translation
        │
        ▼
  Query Execution (MySQL)
        │
        ▼
  Hybrid Post-Processing
  (cleaning + feature engineering)
        │
        ├──► Tabular Result
        └──► Auto-Generated Visualization
```

---

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| Interface | Streamlit |
| Language | Python |
| NLP / Query Translation | NLP-based Natural Language → SQL pipeline |
| Database | MySQL |
| Authentication | Gmail OAuth |

---

## 🚀 Getting Started

### Prerequisites
- Python 3.10+
- MySQL server (hosted)
- A Google Cloud project with OAuth credentials configured for Gmail login

### Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/bi-chatbot.git
cd bi-chatbot

# Create and activate a virtual environment
python -m venv venv
source venv/bin/activate      # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```


## 💡 Usage

1. Sign in with your Gmail account.
2. Upload your business data.
3. Preprocess the data and store in Mysql database online.
4. Type a question in plain English — e.g. *"What were total sales last quarter?"* or *"Which product category performed best this month?"*
5. Get an instant table and chart. No SQL, no manual chart-building.
6. Manage or delete your account data at any time from your profile.

---

## 🗺️ Roadmap

This project is under active development. Planned/upcoming work includes:

- [ ] Smarter automatic chart-type selection for a wider range of query types
- [ ] A higher-performance backend for larger datasets
- [ ] ML-based forecasting features *(planned, not yet implemented — current version supports descriptive querying and visualization of existing data only, not predictive sales forecasting)*
- [ ] Support for additional database connectors beyond MySQL
- [ ] Multi-user team workspaces

> **Note:** The current version answers questions about existing data (descriptive analytics) — it does not yet generate sales forecasts or predictions. Forecasting is a future roadmap item, not a current feature.

---

## 🔒 Data & Privacy

- Authentication is handled via Gmail OAuth — no passwords are stored by this app.
- Each user's data is scoped to their private account ID.
- Users can delete their data at any time.

---

## 🤝 Contributing

This is an actively evolving personal project, and feedback is welcome. If you have suggestions — particularly around automatic visualization logic — feel free to open an issue or reach out.

---

## 📬 Contact

Built by **Muhammad Umar Saleem** — AI/ML Engineer
- Portfolio: (https://umar-saleem.vercel.app/)
- LinkedIn: [linkedin.com/in/yourusername](https://www.linkedin.com/in/umarsaleem49)
- Email: mumarsaleem49@gmail.com

---

## 📄 License

This project is available under the [MIT License](LICENSE).
