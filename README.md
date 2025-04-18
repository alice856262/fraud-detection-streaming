# Real-time Fraud Detection System

> A real-time fraud detection system that uses customer browsing and transaction data to predict fraudulent behavior using a machine learning pipeline integrated with Apache Kafka and Spark Structured Streaming.

## 🚀 System Architecture
![System Diagram](assets/system_architecture.png)

- **Producers**: Simulate real-time browsing and transaction data in batches (500–1000 rows every 5 seconds).
- **Spark Streaming**: Ingests, transforms, and joins streaming + static data; applies a pre-trained fraud detection model.
- **Fraud Detection**: Predicts potential frauds every 10 seconds; aggregates top products from non-fraud sessions.
- **Consumers**: Read from Kafka, visualise trends with bar/line/advanced geospatial plots.

## 🔧 Prerequisites
1. **Install Kafka and Zookeeper:**
```bash
brew install kafka
```

2. **Start Zookeeper and Kafka (in separate terminals):**
```bash
# Terminal 1: Start Zookeeper
zookeeper-server-start /opt/homebrew/etc/kafka/zookeeper.properties

# Terminal 2: Start Kafka
kafka-server-start /opt/homebrew/etc/kafka/server.properties
```

## ⚙️ Setup
1. **Create a virtual environment:**
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

2. **Install dependencies:**
```bash
pip install -r requirements.txt
```

3. **Start the dashboard:**
```bash
streamlit run app/ui.py
```

4. **Access dashboard:** http://localhost:8501

## 🎯 Features
- Real-time data streaming with Kafka
- Feature transformation and ML prediction using PySpark
- Fraud alerts and product trend insights every few seconds
- Visualisations:
    - **Fraud Count Bar Chart**: Updates every 10 seconds
    - **Fraud Alert Location Map**: Bubble map showing fraud density by location
    - **Top Sale Products Line Chart**: Updates every 30 seconds

## 🛠 Tech Stack
- **Streaming**: Apache Kafka, PySpark Structured Streaming
- **Machine Learning**: Pre-trained ML model
- **Visualisation**: Plotly

## 📂 Files Structure
```bash
fraud_streaming_dashboard/
├── app/
│   └── producer.py
│   └── spark_streaming.py
│   └── ui.py                 # Streamlit UI 
├── data/         
├── model/                    # Pre-trained ML model
├── data/
└── requirements.txt
```

## 📦 Important Note
- This system is designed to run locally and requires a local Kafka setup. The Streamlit Cloud deployment will **not** work as it cannot connect to a local Kafka instance.
- To deploy in production:
    - Use a managed Kafka service (e.g., Confluent)
    - Configure Kafka endpoints and security layers
    - Consider containerising the app using Docker or Kubernetes