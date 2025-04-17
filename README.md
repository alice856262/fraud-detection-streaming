# Real-time Fraud Detection System

A real-time fraud detection system that uses customer browsing and transaction data to predict fraudulent behavior.

## Project Structure

```
fraud_streaming_dashboard/
├── app/
│   ├── __init__.py
│   ├── producer.py
│   ├── consumer.py
│   ├── spark_streaming.py
│   └── ui.py
├── model/
│   └── fraud_detection_model_gbt
├── data/
│   ├── FIT5202_A2A_dataset/
│   └── FIT5202_A2B_dataset/
├── requirements.txt
└── README.md
```

## Setup

1. Create a virtual environment:
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Start the application:
```bash
streamlit run app/ui.py
```

## Features

- Real-time data streaming using Apache Kafka
- Fraud detection using machine learning
- Live dashboard with fraud alerts and product analytics
- Interactive visualizations 