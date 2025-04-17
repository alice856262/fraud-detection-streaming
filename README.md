# Real-time Fraud Detection System

A real-time fraud detection system that uses customer browsing and transaction data to predict fraudulent behavior.

## Important Note
This system is designed to run locally and requires a local Kafka setup. The Streamlit Cloud deployment will not work as it cannot connect to a local Kafka instance.

## Prerequisites

1. Install Kafka and Zookeeper:
```bash
brew install kafka
```

2. Start Zookeeper and Kafka (in separate terminals):
```bash
# Terminal 1: Start Zookeeper
zookeeper-server-start /opt/homebrew/etc/kafka/zookeeper.properties

# Terminal 2: Start Kafka
kafka-server-start /opt/homebrew/etc/kafka/server.properties
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

4. Access the dashboard at http://localhost:8501

## Features

- Real-time data streaming using Apache Kafka
- Fraud detection using machine learning
- Live dashboard with fraud alerts and product analytics
- Interactive visualizations

## Note on Deployment
This application is designed for local development and testing. To deploy it to production:
1. Set up a cloud-based Kafka service
2. Update the Kafka configuration to use cloud endpoints
3. Configure security settings for Kafka connections 